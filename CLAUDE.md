# Ryuk

A distributed priority queue. Messages carry a priority 0–100 and an optional
group key; a group is delivered in order, one message at a time. Go 1.25,
module `github.com/harryv2/ryuk-dpq`.

Two binaries. `ryuk-gateway` is stateless: it validates requests, works out
which node owns a queue, forwards, and collects metrics. `ryuk-node` holds the
messages in memory with a write-ahead log behind them. Postgres holds queue
metadata, etcd holds membership, Prometheus scrapes the gateway. The UI is a
Next.js static export served by the gateway.

## Commands

```
make build            go build ./backend/...
make test             go test ./backend/...
make test-race        the one that matters; the engine is concurrent
make vet
make lint-layers      import rules, see below
make up               full stack in docker, 3 nodes
make scale N=6        add nodes
make down             stop and wipe volumes
make infra            postgres + etcd + prometheus only, run the binaries yourself
make dev-env          prints the env vars for an IDE run configuration
make integration      BDD suite against a real docker cluster (~10 min)
make integration-tags TAGS=@scaling
```

Frontend: `cd frontend && npm run build` writes `frontend/out`, which the
gateway serves. A UI change needs that build *and* a gateway image rebuild
before it shows up in docker.

## Layout

```
backend/
  slotting/     which slot a message belongs to - shared by both services
  constants/    limits and defaults
  config/       env and .env loading
  gateway/      controller -> logic -> repo
  queue/        controller -> logic -> repo, plus logic/engine
  queue/logic/engine/   the queue itself: slots, groups, priorities, leases
frontend/       Next.js static export
integration/    godog BDD suite, drives the docker stack through the public API
docs/lld/       design notes
```

Each service is `controller` (parse and validate), `logic` (behaviour), `repo`
(Postgres, etcd, gRPC, WAL), `entity` (types and repo interfaces).

## Import rules — enforced by `make lint-layers`

- `queue/logic/engine` may import `backend/slotting` and **nothing else** from
  the module. It is meant to stay a self-contained core.
- Controllers must not import repo packages.
- `entity` must not import the other layers.
- `gateway` must not import `queue`, and `queue` must not import `gateway`.

Anything both services must agree on goes in `backend/slotting` or
`backend/constants`, never in one service's package.

## Invariants worth knowing before editing

- **A group always resolves to the same slot.** `slotting.SlotFor` is the only
  definition; the gateway's `slotOf` wrapper only adds the "scatter ungrouped
  messages" policy on top.
- **Slot count is fixed for a queue's life** — 16 single-node, 64 distributed.
  Changing it remaps every group.
- **`m.Seq` is assigned under the slot lock.** It orders concurrent producers.
  It is generation-prefixed (`generation<<40 | counter`) so an older owner's
  messages always sort ahead, which is what makes migration merges safe.
- **`slot.take` returns a copy of the message**, not the live pointer. The slot
  keeps mutating the original after the lock is released.
- **`group.locked` is the ordering guarantee.** One message in flight per group.
- **`group.version` and `lease.epoch` are lazy deletion.** Stale entries stay in
  heaps and deques and are skipped when they surface. Do not "tidy" them.
- **`AppendEnqueue` flushes before the message is visible.** The other journal
  calls are buffered. Reversing that loses messages on a crash.
- **A dead letter is moved by the gateway, not the node.** The node holds what
  it gave up on and writes it to `dead-letters.log`; the gateway drains it and
  re-enqueues. The node only drops it once the gateway confirms.
- **No dead-letter queue means never dropping the message.** It keeps being
  redelivered. `engine.Config.HasDeadLetter` is what switches that.
- **Carry a slot id, do not recompute it.** Deriving it from the group key gives
  the wrong answer for an ungrouped message.

## Style

**Comments: only where the code cannot speak for itself.** Explain *why*, never
*what*. No comment that restates the name below it, no section banners, no
scaffolding like `// 1. ... // 2. ...` above obvious steps. A doc comment must
sit on the declaration it describes — several have drifted onto the wrong
function during refactors, and a wrong comment is worse than none.

Write plain prose. No nominalisation, no dramatic one-line fragments, no
marketing tone. Short sentences.

Keep the code simple and modular. No abstraction added for a case that does not
exist yet. Prefer a small function with a clear name over a comment explaining a
long one.

Match the surrounding code: naming, error wrapping (`enterr`), file naming
(`v1-<endpoint>.go` in controllers, `<thing>-logic.go` in logic).

## Working agreements

- A question is a question. Do not change code when asked to explain something.
- Do not write into `docs/lld/` unless asked; present designs in chat.
- Verify with tests and, where it matters, against the running stack. Report
  what actually happened, including failures.
- Mocks are mockgen-generated from the `//go:generate` lines in `entity`.
  Regenerate with `mockgen -source=... -destination=../mocks/... -package=mocks`
  when an interface changes; a stale mock fails the build in a confusing way.

## Gotchas

- Rebuilding node containers gives them new ids, so all existing placement
  points at machines that no longer exist. Queues survive but their data does
  not; delete and reseed after `docker compose up --build`.
- Prometheus publishes 9091 on the host, so dev nodes start at 9110.
- The gateway caches queue config for `RYUK_CACHE_TTL` (30s). With several
  gateways, a settings change can take that long to reach all of them.
- `go test -race` is the one that catches engine bugs. Plain `go test` has
  missed real data races here.
