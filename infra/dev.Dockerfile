# Dev image: source is bind-mounted by docker-compose.override.yml and air rebuilds on change.
FROM golang:1.26-alpine
RUN go install github.com/air-verse/air@v1.62.0
WORKDIR /src
