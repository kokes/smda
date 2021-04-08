package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	endpoint := "localhost:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}

	for j := 0; j < 8; j++ {
		t := time.Now()
		opts := minio.GetObjectOptions{}
		opts.SetRange(50000, 60000)
		ret, err := minioClient.GetObject(context.Background(), "range-testing", "100M", opts)
		if err != nil {
			return err
		}

		if _, err := io.Copy(io.Discard, ret); err != nil {
			return err
		}
		fmt.Printf("elapsed: %v\n", time.Since(t))
	}

	return nil
}
