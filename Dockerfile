FROM golang:1.14-alpine AS build
# technically, we don't need the Makefile, because our build process is very
# simple at this point - but let's keep it, since we don't know what might happen
# in the future
RUN apk --no-cache add make
RUN mkdir -p /smda/
WORKDIR /smda/
COPY Makefile go.mod ./
COPY src src
COPY cmd cmd
COPY samples samples
ENV CGO_ENABLED 0
RUN make build

FROM alpine:latest
RUN mkdir -p /smda/
WORKDIR /smda/
COPY --from=build /smda/server .
# TODO: this will be not necessary once we include artifacts in our binary
COPY --from=build /smda/cmd cmd
COPY --from=build /smda/samples samples

EXPOSE 8822
# TODO: does not bind the port if it's (for any reason) busy
CMD ["./server", "-port", "8822", "-samples"]
