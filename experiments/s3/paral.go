package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return err
	}
	svc := s3.NewFromConfig(cfg)
	ctx := context.Background()

	input := &s3.GetObjectInput{
		Bucket: aws.String("okokes-ranges"),
		Key:    aws.String("100M"),
		// Range:  aws.String("bytes=5-10"),
	}

	workers := 8
	var wg sync.WaitGroup
	wg.Add(workers)
	for j := 0; j < workers; j++ {
		go func() {
			defer wg.Done()
			t := time.Now()
			output, err := svc.GetObject(ctx, input)
			if err != nil {
				log.Fatal(err)
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
				log.Fatal(err)
			}
			fmt.Printf("elapsed: %v\n", time.Since(t))
		}()
	}
	wg.Wait()

	return nil
}
