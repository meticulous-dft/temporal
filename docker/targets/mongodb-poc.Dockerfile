ARG ALPINE_TAG=3.23.5
FROM alpine:${ALPINE_TAG}

ARG TARGETARCH
ARG SOURCE_SHA256

RUN test -n "${SOURCE_SHA256}" && \
    apk add --no-cache ca-certificates tzdata && \
    apk upgrade --no-cache zlib && \
    addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

COPY --chmod=755 ./build/${TARGETARCH}/temporal-server /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal-mongodb-tool /usr/local/bin/
COPY --chmod=755 ./docker/scripts/sh/entrypoint.sh /etc/temporal/entrypoint.sh
COPY ./config/mongodb-poc.yaml /etc/temporal/poc.yaml
COPY ./config/dynamicconfig/mongodb-poc.yaml /etc/temporal/dynamicconfig.yaml

LABEL org.opencontainers.image.title="Temporal MongoDB sharding POC" \
      io.temporal.mongodb.source-sha256="${SOURCE_SHA256}" \
      io.temporal.mongodb.storage-model="bucketed_atomic_v1" \
      io.temporal.mongodb.sharding-plan="5" \
      io.temporal.mongodb.storage-buckets="64"

ENV BIND_ON_IP=0.0.0.0 \
    TEMPORAL_SERVER_CONFIG_FILE_PATH=/etc/temporal/poc.yaml

WORKDIR /etc/temporal
USER temporal
CMD ["/etc/temporal/entrypoint.sh"]
