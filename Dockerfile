FROM --platform=linux/amd64 golang:1.21

WORKDIR /go/src/app

# Install 7zip
RUN apt-get update && apt-get install -y p7zip-full

COPY go.mod go.sum .
RUN go get -d -v ./...

COPY cmd cmd
COPY pkg pkg
RUN go install -v ./cmd/scraper

CMD ["scraper"]