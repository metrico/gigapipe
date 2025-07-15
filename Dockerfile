FROM golang:1.24.0-alpine as builder
COPY . /src
WORKDIR /src
ARG VIEW
RUN if [ "$VIEW" = "1" ]; then \
        go build -tags view -o gigapipe cmd/gigapipe/main.go ; \
    else \
        go build -o gigapipe cmd/gigapipe/main.go ; \
    fi

FROM alpine:3.21
COPY --from=builder /src/gigapipe /gigapipe
ENV PORT 3100
EXPOSE 3100
CMD /gigapipe
