package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

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

func lambdaRequestToNative(req events.LambdaFunctionURLRequest) *http.Request {
	header := make(http.Header, len(req.Headers))
	for k, v := range req.Headers {
		header.Set(k, v) // `Add` would've done the same
	}
	ret := http.Request{
		Method:        req.RequestContext.HTTP.Method,
		Proto:         req.RequestContext.HTTP.Protocol,
		RemoteAddr:    req.RequestContext.HTTP.SourceIP,
		Body:          io.NopCloser(strings.NewReader(req.Body)),
		ContentLength: int64(len(req.Body)),
		Header:        header,
		URL: &url.URL{
			Scheme:   "https",
			Host:     req.RequestContext.DomainName,
			Path:     req.RequestContext.HTTP.Path,
			RawPath:  req.RawPath,
			RawQuery: req.RawQueryString,
		},
	}
	// TODO: check if we need to parse cookies from req.Cookies or if they're extracted
	// from headers automatically
	return &ret
}

func HandleRequest(ctx context.Context, req events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
	invocations += 1
	// TODO: use `embed` to return static assets

	// maybe instead construct a http.Request instead and pass it to our web.handlers
	// call `lambdaRequestToNative` and create a recording ResponseWriter
	// and then wrap this by our native web handlers
	// hint: should we use httptest.ResponseRecorder instead?

	if req.RawPath == "/" {
		return events.LambdaFunctionURLResponse{
			StatusCode: http.StatusOK,
			Headers: map[string]string{
				"Content-Type": "text/html",
			},
			Body: fmt.Sprintf("<h1>Hello</h1>\n%v", jsMeasure),
		}, nil
	}
	if req.RawPath == "/introspect" {
		return events.LambdaFunctionURLResponse{
			StatusCode: http.StatusOK,
			Body:       fmt.Sprintf("request: %+v\n", req),
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
