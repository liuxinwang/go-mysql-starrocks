# syntax=docker/dockerfile:1
FROM golang:1.18-alpine
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk add --no-cache \
    wget \
    make \
    git \
    gcc \
    musl-dev
WORKDIR /app
COPY go.mod go.sum ./
ENV GOPROXY "https://goproxy.cn/"
ENV GO111MODULE "on"
RUN go mod download
COPY pkg ./
COPY cmd/go_mysql_sr/main.go ./
RUN ls -lh

RUN CGO_ENABLED=0 GOOS=linux go build -o /go-mysql-sr

CMD ["/go-mysql-sr -config=/etc/go-mysql-sr/starrocks.toml"]