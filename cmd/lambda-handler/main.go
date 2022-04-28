package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/web"
)

var invocations int

var db *database.Database

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
	if db == nil {
		t := time.Now()
		var err error
		db, err = database.NewDatabase("", nil)
		if err != nil {
			// TODO: write a wrapper to return this as a 500
			panic(err.Error())
		}
		log.Printf("db init took %v", time.Since(t)) // TODO: remove
	}

	// in the end I think we'll do this:
	// 1) create a smda db (perhaps in the main() loop or init(), not sure)
	// 2) convert a lambdaFunctionURL request to net/http.Request
	// 3) initialise a recording responsewriter (maybe use httptest.ResponseRecorder)
	// 4) run web.setupRoutes (need to expose it first)
	// 5) run router.ServeHTTP(mockWriter, convertedRequest)
	// 6) convert mockWriter response into a lambdaFunctionURL response
	// 7) remove all disk I/O from NewDatabase

	handler := web.SetupRoutes(db)
	_ = handler
	// TODO: steps 3, 5, 6, maybe 7

	if req.RawPath == "/" {
		return events.LambdaFunctionURLResponse{
			StatusCode: http.StatusOK,
			Headers: map[string]string{
				"Content-Type": "text/html",
			},
			Body: fmt.Sprintf("<h1>Hello again</h1>\n%v", jsMeasure),
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
