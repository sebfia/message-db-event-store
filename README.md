# message-db-event-store
Dotnet event store making use of the PostgreSQL message-db of eventide and NATS.
# To build docker image for amd64 and arm64 run:
    docker buildx build --no-cache  -f src/Server/Dockerfile -t ghcr.io/sebfia/message-db-event-store:0.1.0 --platform linux/amd64,linux/arm64 --push .
