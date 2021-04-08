package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// yeah yeah yeah, it's clunky
	resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: "https://fra1.digitaloceanspaces.com",
		}, nil
	})

	creds := credentials.NewStaticCredentialsProvider(os.Getenv("DO_SPACES_KEY"), os.Getenv("DO_SPACES_SECRET"), "")
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(creds),
		config.WithRegion("fra1"),
		config.WithEndpointResolver(resolver),
	)
	if err != nil {
		return err
	}
	fmt.Printf("got this region: %v\n", cfg.Region)
	svc := s3.NewFromConfig(cfg)
	ctx := context.Background()

	input := &s3.GetObjectInput{
		Bucket: aws.String("range-testing"),
		Key:    aws.String(os.Args[1]),
		Range:  aws.String(os.Args[2]), // bytes=5-10
	}

	for j := 0; j < 8; j++ {
		t := time.Now()
		output, err := svc.GetObject(ctx, input)
		if err != nil {
			return err
		}
		defer output.Body.Close()
		if output.AcceptRanges != nil {
			fmt.Printf("was range specified?: %v\n", *output.AcceptRanges)
		}
		if output.ContentRange != nil {
			fmt.Printf("this was the range: %v\n", *output.ContentRange)
		}
		// if _, err := io.Copy(os.Stdout, output.Body); err != nil {
		if _, err := io.Copy(io.Discard, output.Body); err != nil {
			return err
		}
		fmt.Printf("elapsed: %v\n", time.Since(t))
	}

	return nil
}
