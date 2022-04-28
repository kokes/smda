package main

import (
	"bytes"
	"context"
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

var dummyStatusCode int = -1

type recordingResponseWriter struct {
	headers http.Header
	buffer  bytes.Buffer
	status  int
}

func newRecordingResponseWriter() *recordingResponseWriter {
	return &recordingResponseWriter{
		headers: make(http.Header),
		status:  dummyStatusCode,
	}
}

func (rw *recordingResponseWriter) Header() http.Header {
	return rw.headers
}

func (rw *recordingResponseWriter) WriteHeader(statusCode int) {
	rw.status = statusCode
}

func (rw *recordingResponseWriter) Write(s []byte) (int, error) {
	if rw.status == dummyStatusCode {
		rw.status = http.StatusOK
	}

	// TODO: detect MIME via http.DetectContentType?

	return rw.buffer.Write(s)
}

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

// TODO: err if we don't have status set? (in case of no writes)
func (rw *recordingResponseWriter) toLambdaFunctionResponse() events.LambdaFunctionURLResponse {
	headers := make(map[string]string)
	for h, v := range rw.headers {
		headers[h] = strings.Join(v, ",")
	}
	ret := events.LambdaFunctionURLResponse{
		StatusCode:      rw.status,
		Body:            rw.buffer.String(),
		IsBase64Encoded: false,
		Headers:         headers,
		// Cookies: , // TODO?
	}

	return ret
}

func HandleRequest(ctx context.Context, req events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
	invocations += 1
	if db == nil {
		t := time.Now()
		var err error
		// TODO: remove all disk I/O from db creation
		db, err = database.NewDatabase("", nil)
		if err != nil {
			// TODO: write a wrapper to return this as a 500
			panic(err.Error())
		}
		log.Printf("db init took %v", time.Since(t)) // TODO: remove
	}

	// what happens now is:
	// 1) convert a lambdaFunctionURL request to net/http.Request
	// 2) initialise a recording responsewriter
	// 3) run our smda.router.ServeHTTP(mockWriter, convertedRequest)
	// 4) convert mockWriter response into a lambdaFunctionURL response

	handler := web.SetupRoutes(db)
	rw := newRecordingResponseWriter()
	httpReq := lambdaRequestToNative(req)
	handler.ServeHTTP(rw, httpReq)

	return rw.toLambdaFunctionResponse(), nil
}

func main() {
	lambda.Start(HandleRequest)
}
