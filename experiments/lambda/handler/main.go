package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

var invocations int

var jsMeasure string = `
Measure container reuse by running something like this
<pre>
for (let j=0; j < 10; j++) {
	const stats = document.createElement("span");
	stats.innerText = (await (await fetch("/data")).json()).invocations + ", ";
	document.body.appendChild(stats);
}
</pre>
`

func HandleRequest(ctx context.Context, req events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
	invocations += 1
	// TODO: use `embed` to return static assets
	if req.RawPath == "/" {
		return events.LambdaFunctionURLResponse{
			StatusCode: http.StatusOK,
			Headers: map[string]string{
				"Content-Type": "text/html",
			},
			Body: fmt.Sprintf("<h1>Hello</h1>\n%v", jsMeasure),
		}, nil
	}

	return events.LambdaFunctionURLResponse{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: fmt.Sprintf("{\"invocations\": %v}\n", invocations),
	}, nil
}

func main() {
	lambda.Start(HandleRequest)
}
