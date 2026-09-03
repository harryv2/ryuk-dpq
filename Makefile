.PHONY: build test test-race vet up down scale logs clean integration integration-fast \
	infra infra-down infra-logs run-gateway run-node dev-env

build:
	go build ./backend/...

test:
	go test ./backend/...

test-race:
	go test -race ./backend/...

vet:
	go vet ./backend/...

# docker compose up, three nodes by default
up:
	cd deploy && docker compose up -d --build --scale node=3

# ---------------------------------------------------------------------------
# Running the Go processes yourself, with only the infrastructure in Docker.
# Postgres and etcd publish their ports; Prometheus scrapes the host, so the
# metrics charts still work while the gateway is under a debugger.

DEV := -f docker-compose.yml -f docker-compose.dev.yml

infra:
	cd deploy && docker compose $(DEV) up -d postgres etcd prometheus
	@echo
	@echo "  postgres    localhost:5432   ryuk / ryuk / ryuk"
	@echo "  etcd        localhost:2379"
	@echo "  prometheus  http://localhost:9091"
	@echo
	@echo "  now run the gateway and a node -- 'make dev-env' prints the variables"

infra-down:
	cd deploy && docker compose $(DEV) down

infra-logs:
	cd deploy && docker compose $(DEV) logs -f postgres etcd prometheus

# The gateway's defaults already point at localhost, so it needs little else.
run-gateway:
	RYUK_PROMETHEUS=http://localhost:9091 \
	RYUK_STATIC_DIR=frontend/out \
	RYUK_LOG_FORMAT=text \
	go run ./backend/cmd/ryuk-gateway

# make run-node N=2  -- a second node on its own port with its own data.
# Ports start at 9110, not 9090: Prometheus publishes 9091 on the host, so
# numbering from 9090 would collide with it on the second node.
N ?= 1
NODE_PORT = $(shell expr 9109 + $(N))
run-node:
	RYUK_LISTEN=:$(NODE_PORT) \
	RYUK_ADVERTISE=localhost:$(NODE_PORT) \
	RYUK_DATA_DIR=./data/dev/node$(N) \
	RYUK_ETCD=localhost:2379 \
	RYUK_LOG_FORMAT=text \
	go run ./backend/cmd/ryuk-node

# Paste these into an IDE run configuration.
dev-env:
	@echo "gateway:"
	@echo "  RYUK_POSTGRES=postgres://ryuk:ryuk@localhost:5432/ryuk?sslmode=disable"
	@echo "  RYUK_ETCD=localhost:2379"
	@echo "  RYUK_PROMETHEUS=http://localhost:9091"
	@echo "  RYUK_STATIC_DIR=frontend/out"
	@echo "  RYUK_LOG_FORMAT=text"
	@echo
	@echo "node -- one run configuration per instance:"
	@echo "  RYUK_LISTEN=:9110           # 9111, 9112 ... for more"
	@echo "  RYUK_ADVERTISE=localhost:9110"
	@echo "  RYUK_DATA_DIR=./data/dev/node1"
	@echo "  RYUK_ETCD=localhost:2379"
	@echo "  RYUK_LOG_FORMAT=text"

# make scale N=6
scale:
	cd deploy && docker compose up -d --scale node=$(N)

down:
	cd deploy && docker compose down -v

logs:
	cd deploy && docker compose logs -f gateway node

clean:
	rm -rf data data-it

# The full stack in Docker, driven through the public API. Builds the images,
# runs every scenario, tears it down. Uses its own port and data directory so a
# development stack can stay up.
integration:
	cd integration && go test -v -timeout 25m

# The same, reusing the images that are already built.
integration-fast:
	cd integration && RYUK_IT_NO_BUILD=1 go test -v -timeout 25m

# make integration-tags TAGS=@failure
integration-tags:
	cd integration && go test -v -timeout 25m -godog.tags=$(TAGS)

# The engine must stay free of everything outside the standard library, and the
# layers must not reach past each other. This is the rule that erodes first.
lint-layers:
	@! grep -rl '"github.com/harryv2/ryuk-dpq' backend/queue/logic/engine \
		| grep -q . || (echo "engine must not import from the module"; exit 1)
	@! grep -rn 'gateway/repo\|queue/repo' backend/*/controller/*.go 2>/dev/null \
		| grep -q . || (echo "controllers must not import repo"; exit 1)
	@! grep -rn 'gateway/logic\|gateway/repo\|gateway/controller' backend/gateway/entity/*.go 2>/dev/null \
		| grep -q . || (echo "entity must not import the other layers"; exit 1)
	@! grep -rn 'backend/gateway' backend/queue --include='*.go' 2>/dev/null \
		| grep -q . || (echo "queue must not import gateway"; exit 1)
	@! grep -rn 'backend/queue' backend/gateway --include='*.go' 2>/dev/null \
		| grep -q . || (echo "gateway must not import queue"; exit 1)
	@echo "layers ok"
