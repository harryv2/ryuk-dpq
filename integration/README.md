# Integration tests

These run the real system. Docker Compose brings up Postgres, etcd, the gateway
and however many nodes the run asks for, and every assertion goes through the
public REST API — the same one a caller would use. Nothing is stubbed.

The scenarios are written in Gherkin under `features/` and executed by
[godog](https://github.com/cucumber/godog), so what is being checked reads as
prose and the Go in `steps_test.go` only says how to check it.

## Running

```bash
make integration
```

or directly:

```bash
cd integration && go test -v -timeout 20m
```

The suite builds the images, starts the stack, runs every feature, and tears the
stack down again.

## Flags

| Flag or variable | Default | What it does |
|---|---|---|
| `-nodes` | 3 | how many queue nodes to start |
| `-keep-up` | off | leave the stack running after the suite |
| `-godog.tags` | — | run a subset, e.g. `-godog.tags=@failure` |
| `RYUK_IT_NO_BUILD` | — | skip the image build, for a fast rerun |
| `RYUK_IT_PORT` | 8091 | host port for the suite's gateway |
| `RYUK_IT_DATA` | `../data-it` | where the suite's stack writes |
| `RYUK_IT_VERBOSE` | — | show Docker output |

The suite runs on its own project name, port and data directory, so it does not
disturb a development stack started with `make up`.

## Layout

```
features/            the scenarios, in Gherkin
steps_test.go        what each step does
suite_test.go        starts the cluster, runs the suite, tears it down
cluster/             Docker Compose lifecycle: up, scale, stop a node, down
client/              a REST client for the gateway
```

## What is covered

| Feature | What it proves |
|---|---|
| `queue-admin` | creation, idempotent creation, deletion, and every validation that runs before anything is stored |
| `priority-and-ordering` | priority order, FIFO within a priority, group ordering, group exclusivity, visibility timeout, nack |
| `distributed-queues` | slots spread over the cluster, exactly-once delivery, group order across machines, exact versus summed counts |
| `scaling-and-failure` | adding a node rebalances, losing a node costs only its slots, a restarted node replays its log |
| `tenancy` | the org comes from the credential; two tenants can share a queue name and see nothing of each other |
| `dead-letter` | retries run out, delayed delivery holds a message back |

Scenarios that stop or add containers are tagged `@failure` and `@scaling`. They
put the cluster back the way they found it, so the rest of the suite is
unaffected.
