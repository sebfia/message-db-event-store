FROM mcr.microsoft.com/dotnet/sdk:6.0 AS builder
RUN dotnet tool install -g paket
ENV PATH="/root/.dotnet/tools:${PATH}"
COPY src/*.fs /src/
COPY src/ThinLayer/*.fs /src/ThinLayer/
COPY src/ThinLayer/*.fsproj /src/ThinLayer/
COPY src/ThinLayer/paket.references /src/ThinLayer/
COPY src/Server/*.fs /src/Server/
COPY src/Server/*.fsproj /src/Server/
COPY src/Server/paket.references /src/Server/
COPY paket.lock /src/
COPY paket.dependencies /src/
WORKDIR /src
RUN paket install
WORKDIR /src/Server
RUN dotnet restore
RUN dotnet build -c Release
FROM builder AS publish
ARG TARGETPLATFORM
RUN echo "I am building for $TARGETPLATFORM"
RUN if [ "${TARGETPLATFORM}" = "linux/amd64" ] ; \
    then \ 
        dotnet publish -c Release -r linux-musl-x64 --self-contained true /p:PublishSingleFile=true /p:EnableCompressionInSingleFile=true /p:PublishTrimmed=true -o /app ; \
    else \
        dotnet publish -c Release -r linux-musl-arm64 --self-contained true /p:PublishSingleFile=true /p:EnableCompressionInSingleFile=true /p:PublishTrimmed=true -o /app ; \
    fi
# RUN dotnet publish -c Release -r linux-musl-arm64 --self-contained true /p:PublishSingleFile=true /p:EnableCompressionInSingleFile=true /p:PublishTrimmed=true -o /app
FROM --platform=$TARGETPLATFORM alpine:3.15
LABEL maintainer="Sebastian Fialka <sebastian.fialka@sebfia.net>"
RUN apk add --no-cache libstdc++ krb5-libs libintl tzdata
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
ENV CONNECTION_STRING="Host=localhost;Port=5432;Database=message_store;Username=postgres;Password=summersun"
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["/app/eventstore"] 