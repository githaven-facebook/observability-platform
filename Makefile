BINARY_DIR := bin
COLLECTOR_BINARY := $(BINARY_DIR)/collector
QUERY_BINARY := $(BINARY_DIR)/query
ALERTMANAGER_BINARY := $(BINARY_DIR)/alertmanager

GO := go
GOFLAGS := -trimpath
LDFLAGS := -ldflags="-s -w"

.PHONY: all build test lint clean run-collector run-query run-alertmanager docker-build

all: build

build: $(COLLECTOR_BINARY) $(QUERY_BINARY) $(ALERTMANAGER_BINARY)

$(BINARY_DIR):
	mkdir -p $(BINARY_DIR)

$(COLLECTOR_BINARY): $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/collector/...

$(QUERY_BINARY): $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/query/...

$(ALERTMANAGER_BINARY): $(BINARY_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/alertmanager/...

test:
	$(GO) test ./... -v -race -coverprofile=coverage.out
	$(GO) tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

clean:
	rm -rf $(BINARY_DIR)
	rm -f coverage.out coverage.html

run-collector: $(COLLECTOR_BINARY)
	$(COLLECTOR_BINARY) --config config/collector.yaml

run-query: $(QUERY_BINARY)
	$(QUERY_BINARY) --config config/query.yaml

run-alertmanager: $(ALERTMANAGER_BINARY)
	$(ALERTMANAGER_BINARY) --config config/alertmanager.yaml

docker-build:
	docker build -f Dockerfile.collector -t observability-collector:latest .
	docker build -f Dockerfile.query -t observability-query:latest .
	docker build -f Dockerfile.alertmanager -t observability-alertmanager:latest .

.PHONY: tidy
tidy:
	$(GO) mod tidy
