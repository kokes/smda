FROM golang:1.16-alpine AS build
# technically, we don't need the Makefile, because our build process is very
# simple at this point - but let's keep it, since we don't know what might happen
# in the future
RUN apk --no-cache add make
RUN mkdir -p /smda/
WORKDIR /smda/
COPY Makefile go.mod go.sum ./
COPY src src
COPY cmd cmd
RUN make build

FROM scratch
# ARCH: this is a bit of a hack to ensure that /tmp/ exists wihout having to create
# it in alpine and copying it over (we need it for temporary directories)
# We might as well use alpine as the base image
WORKDIR /tmp/
COPY --from=build /smda/server .

EXPOSE 8822
CMD ["./server", "-port-http", "8822", "-expose", "-samples"]
