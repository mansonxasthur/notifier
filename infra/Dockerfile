# Production image for one entry point: docker build --build-arg SERVICE=api -f infra/Dockerfile .
FROM golang:1.26-alpine AS build
ARG SERVICE
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN test -n "$SERVICE" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
COPY config/config.yaml /etc/notifier/config.yaml
ENV CONFIG_PATH=/etc/notifier/config.yaml
ENTRYPOINT ["/app"]
