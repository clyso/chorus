FROM golang:1.21 as builder
# FROM --platform=$BUILDPLATFORM golang:1.21 as builder
ARG GIT_COMMIT='not set'
ARG GIT_TAG=development
ENV GIT_COMMIT=$GIT_COMMIT
ENV GIT_TAG=$GIT_TAG

WORKDIR /build

COPY ./go.mod go.mod
COPY ./go.sum go.sum

RUN go mod download

COPY . .

ARG GOOS=linux
# amd64| arm64
# ARG GOARCH=amd64 
# worker|proxy|agent
ARG SERVICE=worker

RUN CGO_ENABLED=0 GO111MODULE=on GOOS=$GOOS GOARCH=${TARGETARCH} go build -ldflags="-X 'main.version=$GIT_TAG' -X 'main.commit=$GIT_COMMIT'" -o chorus ./cmd/${SERVICE}

# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
# FROM --platform=$TARGETPLATFORM gcr.io/distroless/static:nonroot
USER nonroot:nonroot
WORKDIR /bin

COPY --chown=nonroot:nonroot --from=builder /build/chorus chorus

CMD ["chorus"]
