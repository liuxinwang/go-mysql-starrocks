FROM golang:1.18-alpine as builder
ENV TZ=Asia/Shanghai
ENV LANG="en_US.UTF-8"
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
WORKDIR /app
ENV GOPROXY "https://goproxy.cn,direct"
ENV GO111MODULE "on"
COPY . ./
RUN go mod download && go mod verify
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /go-mysql-sr ./cmd/go_mysql_sr/main.go

FROM alpine
ENV TZ=Asia/Shanghai
ENV LANG="en_US.UTF-8"
WORKDIR /app
COPY --from=builder /go-mysql-sr ./go-mysql-sr

RUN set -x \
  && apk add --no-cache tzdata bash \
  && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ["./go-mysql-sr", "-config", "/etc/go-mysql-sr/starrocks.toml"]