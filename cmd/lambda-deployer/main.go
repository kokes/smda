// This is an ad-hoc script to set up all the necessary AWS
// services. In case this Lambda approach is viable, maybe
// include some CloudFormation templates, perhaps Terraform,
// Pulumi etc.
package main

import (
	"context"
	"errors"
	"log"
	"os"

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

// TODO: allow s3?
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
	if len(os.Args) != 2 {
		return errors.New("need to supply the lambda zip bundle as the first and only argument")
	}
	lambdaPkg := os.Args[1]
	zipData, err := os.ReadFile(lambdaPkg)
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
		// TODO: unescape and load *getRole.Role.AssumeRolePolicyDocument and compare to iamPolicy
		// https://github.com/aws/aws-sdk-go-v2/issues/225
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

	// update if exists
	_, err = lambdaClient.GetFunction(context.TODO(), &lambda.GetFunctionInput{
		FunctionName: &functionName,
	})

	if err == nil {
		log.Printf("function exists, updating function code")
		lambdaClient.UpdateFunctionCode(context.TODO(), &lambda.UpdateFunctionCodeInput{
			FunctionName: &functionName,
			ZipFile:      zipData,
		})
	}

	var lexists *lambdaTypes.ResourceNotFoundException
	if err != nil {
		if !errors.As(err, &lexists) {
			return err
		}
		log.Printf("lambda does not exist, creating")
		// TODO: these don't get overriden in case the function already exists
		// maybe add some "--recreate" mode
		lambdaInputs := &lambda.CreateFunctionInput{
			FunctionName: &functionName,
			Role:         role.Arn,
			Runtime:      lambdaTypes.RuntimeGo1x,
			Handler:      aws.String("main"), // TODO: param/global
			Code: &lambdaTypes.FunctionCode{
				ZipFile: zipData,
			},
			Timeout: aws.Int32(30), // TODO
			// TODO: environment
			// TODO: memory and such
		}
		fn, err := lambdaClient.CreateFunction(context.TODO(), lambdaInputs)
		if err != nil {
			return err
		}
		log.Printf("function created: %v", *fn.FunctionArn)

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
	}

	// get metadata
	urlc, err := lambdaClient.GetFunctionUrlConfig(context.TODO(), &lambda.GetFunctionUrlConfigInput{
		FunctionName: &functionName,
	})
	if err != nil {
		return err
	}
	log.Printf("lambda URL: %v", *urlc.FunctionUrl)

	// TODO: test that the URL works now

	return nil
}
