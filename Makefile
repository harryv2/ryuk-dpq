.PHONY: build test test-race vet up down scale logs clean integration integration-fast

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
