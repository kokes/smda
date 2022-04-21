package main

import (
	"archive/zip"
	"context"
	"errors"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdaTypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// TODO: allow logs? s3?
// in that case add AWSLambdaBasicExecutionRole somehow
var iamPolicy string = `{
    "Version": "2012-10-17",
	"Statement": [
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
			"Principal": {"Service": "lambda.amazonaws.com"}
        }
    ]
}`

var attachRoles []string = []string{
	"arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole", // basic logging permissions
}

func run() error {
	// 0) build our function
	// TODO: don't build it here, have it as part of our `make dist` and only use the (zipped) binary
	//       directly from here
	log.Println("building our Lambda function")
	// TODO: parametrise? run `which`?
	binPath := "./main" // TODO: extract filename to `-o` and `Dir` below?
	binAbsPath, err := filepath.Abs(binPath)
	if err != nil {
		return err
	}
	cmd := exec.Command("go", "build", "-o", binAbsPath, ".")
	cmd.Env = append(os.Environ(), []string{"GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"}...) // TODO: arm64 functions?
	cmd.Dir = "handler"
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	defer os.Remove(binPath)
	log.Printf("built %v", binPath)
	log.Println("compressing")
	fw, err := os.Create("main.zip")
	if err != nil {
		return err
	}
	defer os.Remove("main.zip")
	zw := zip.NewWriter(fw)
	fi, err := os.Stat(binPath)
	if err != nil {
		return err
	}
	fh, err := zip.FileInfoHeader(fi)
	if err != nil {
		return err
	}
	w, err := zw.CreateHeader(fh)
	if err != nil {
		return err
	}
	// TODO: maybe use io.Copy instead (and defer closing and deletion) - once we move
	// it into a function
	binFile, err := os.ReadFile(binPath)
	if err != nil {
		return err
	}
	if _, err := w.Write(binFile); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	if err := fw.Close(); err != nil {
		return err
	}

	zipData, err := os.ReadFile("main.zip")
	if err != nil {
		return err
	}

	// 1) setup config
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("eu-central-1"),          // TODO: flag
		config.WithSharedConfigProfile("personal"), // TODO: flag
	)
	if err != nil {
		return err
	}
	log.Printf("config loaded for region %v", cfg.Region)

	// 2) create an iam role (TODO: func getOrCreateRole())
	roleName := "my_execution_role" // TODO: flag
	var role *iamTypes.Role
	iamClient := iam.NewFromConfig(cfg)
	log.Printf("getting role %v", roleName)
	getRole, err := iamClient.GetRole(context.TODO(), &iam.GetRoleInput{RoleName: &roleName})
	if err == nil {
		log.Printf("role exists")
		role = getRole.Role
	}
	var exists *iamTypes.NoSuchEntityException
	if err != nil {
		if !errors.As(err, &exists) {
			return err
		}
		log.Printf("role does not exist, creating")
		roleInputs := &iam.CreateRoleInput{
			RoleName:                 aws.String(roleName),
			AssumeRolePolicyDocument: &iamPolicy,
		}
		createRole, err := iamClient.CreateRole(context.TODO(), roleInputs)
		if err != nil {
			return err
		}
		role = createRole.Role
		// TODO: the role doesn't exist for the next few seconds... we may have to check for its existence here and wait
	}

	for _, arole := range attachRoles {
		if _, err := iamClient.AttachRolePolicy(context.TODO(), &iam.AttachRolePolicyInput{
			RoleName:  &roleName,
			PolicyArn: &arole,
		}); err != nil {
			return err
		}
		log.Printf("attached policy %v", arole)
	}

	log.Printf("we have a (new) role: %+v", *role.Arn)

	// 3) create a lambda function
	functionName := "smda-gateway" // TODO: formalise
	lambdaClient := lambda.NewFromConfig(cfg)

	// TODO: let's not delete it every single time (add a version or update config instead)
	log.Printf("deleting function %v", functionName)
	lambdaClient.DeleteFunction(context.TODO(), &lambda.DeleteFunctionInput{
		FunctionName: &functionName,
	})

	// TODO: update if exists (given a flag, create only by default)
	// TODO: description etc.
	lambdaInputs := &lambda.CreateFunctionInput{
		FunctionName: &functionName,
		Role:         role.Arn,
		Runtime:      lambdaTypes.RuntimeGo1x,
		// TODO: extract binary name from binPath
		Handler: aws.String("main"),
		Code: &lambdaTypes.FunctionCode{
			ZipFile: zipData,
		},
		// TODO: environment
		// TODO: memory and such
		// TODO: timeout
	}
	fn, err := lambdaClient.CreateFunction(context.TODO(), lambdaInputs)
	if err != nil {
		return err
	}
	log.Printf("function created: %v", *fn.FunctionArn)
	_, err = lambdaClient.GetFunctionUrlConfig(context.TODO(), &lambda.GetFunctionUrlConfigInput{FunctionName: &functionName})
	if err == nil {
		log.Println("already have a function URL, deleting")
		if _, err := lambdaClient.DeleteFunctionUrlConfig(context.TODO(), &lambda.DeleteFunctionUrlConfigInput{FunctionName: &functionName}); err != nil {
			return err
		}
	}
	fu, err := lambdaClient.CreateFunctionUrlConfig(context.TODO(), &lambda.CreateFunctionUrlConfigInput{
		FunctionName: &functionName,
		AuthType:     lambdaTypes.FunctionUrlAuthTypeNone,
		// Cors: // TODO
	})
	if err != nil {
		return err
	}
	log.Printf("function URL created: %v", *fu.FunctionUrl)

	// update permissions, so that this Function URL can be invoked
	perm, err := lambdaClient.AddPermission(context.TODO(), &lambda.AddPermissionInput{
		FunctionName:        &functionName,
		Action:              aws.String("lambda:InvokeFunctionUrl"),
		Principal:           aws.String("*"),
		StatementId:         aws.String("FunctionURLAllowPublicAccess"),
		FunctionUrlAuthType: lambdaTypes.FunctionUrlAuthTypeNone,
	})
	if err != nil {
		return err
	}
	log.Printf("added permission: %v", *perm.Statement)

	// TODO: test that the URL works now

	return nil
}
