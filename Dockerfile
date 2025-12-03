FROM golang:1.24-alpine

WORKDIR /app

COPY . ./

RUN go mod tidy
RUN go build -o /bin/distributedcounter

EXPOSE 8080

ENTRYPOINT ["distributedcounter"]
