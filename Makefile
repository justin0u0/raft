PATH := $(CURDIR)/bin:$(PATH)

.PHONY: proto
proto: bin/protoc-gen-go bin/protoc-gen-go-grpc
	protoc \
		-I . \
		--go_out=paths=source_relative:. \
		--go-grpc_out=paths=source_relative:. \
		./pb/*.proto

bin/protoc-gen-go: go.mod
	go build -o $@ google.golang.org/protobuf/cmd/protoc-gen-go

bin/protoc-gen-go-grpc: go.mod
	go build -o $@ google.golang.org/grpc/cmd/protoc-gen-go-grpc
