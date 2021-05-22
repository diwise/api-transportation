FROM golang:1.16 AS builder


# Set the Current Working Directory inside the container
WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

WORKDIR /app/cmd/api-transportation

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build


FROM registry.access.redhat.com/ubi8/ubi-minimal
WORKDIR /opt/diwise

COPY --from=builder --chown=1001 /app/cmd/api-transportation/api-transportation /opt/diwise
COPY --chown=1001 assets/segments.db /opt/diwise/segments.db

RUN chown 1001 /opt/diwise
RUN chmod 700 /opt/diwise

EXPOSE 8080
USER 1001

ENTRYPOINT ["/opt/diwise/api-transportation", "-segsfile", "/opt/diwise/segments.db"]
