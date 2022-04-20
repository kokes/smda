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

// TODO: allow logs? s3?
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

func run() error {
	log.Println("it runs!")
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
	}
	log.Printf("we have a (new) role: %+v", *role.Arn)

	// 3) create a lambda function
	zipData, err := os.ReadFile("main.zip") // TODO: create
	if err != nil {
		return err
	}
	functionName := "ahoy" // TODO: formalise
	lambdaClient := lambda.NewFromConfig(cfg)

	// TODO: let's not delete it every single time
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
		Handler:      aws.String("MyHandler"), // TODO: set
		Code: &lambdaTypes.FunctionCode{
			ZipFile: zipData,
		},
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

	return nil
}
