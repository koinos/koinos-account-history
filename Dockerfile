FROM golang:1.16.2-alpine as builder

ADD . /koinos-contract-meta-store
WORKDIR /koinos-contract-meta-store

RUN go get ./... && \
    go build -o koinos_contract_meta_store cmd/koinos-contract-meta-store/main.go

FROM alpine:latest
COPY --from=builder /koinos-contract-meta-store/koinos_contract_meta_store /usr/local/bin
ENTRYPOINT [ "/usr/local/bin/koinos_contract_meta_store" ]
