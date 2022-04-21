package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

// type MyEvent struct {
//         Name string `json:"name"`
// }

func HandleRequest(ctx context.Context, req events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
	if req.RawPath == "/" {
		return events.LambdaFunctionURLResponse{
			StatusCode: 200,
			Headers: map[string]string{
				"Content-Type": "text/html",
			},
			Body: "<h1>Hello</h1>",
		}, nil
	}
	log.Printf("got this request: %+v", req)
	return events.LambdaFunctionURLResponse{
		StatusCode: 200,
		Headers:    nil,
		Body:       "ahoy!\n",
	}, nil
}

func main() {
	lambda.Start(HandleRequest)
}
