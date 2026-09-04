# Postman collection

Every endpoint the gateway serves: 22 requests in five folders.

## Import

Postman → Import → both files in this directory:

- `ryuk.postman_collection.json`
- `ryuk.postman_environment.json` — sets `baseUrl`, `token` and `queue`

Then pick **Ryuk — local** in the environment dropdown. Start the stack with
`make up` first; the gateway listens on 8090.

## Try it in this order

1. **Queues → Create queue (single node)**
2. **Messages → Send a message**
3. **Messages → Poll for messages** — a test script saves the receipt into the
   `receipt` variable
4. **Messages → Acknowledge** — uses that receipt, so nothing is copied by hand

**Cluster → Cluster overview** does the same for `nodeId`, so **Node detail**
works straight after it.

## Variables

| | |
|---|---|
| `baseUrl` | `http://localhost:8090` |
| `token` | `acme-token`, or `globex-token` to see the same queue names in another tenant |
| `queue` | which queue the message and stats requests act on |
| `receipt` | filled in by the poll request |
| `nodeId` | filled in by the cluster overview |

The bearer token *is* the tenant — there is no other tenancy check, and the org
is taken from the credential rather than from any request field.

## Command line

The collection runs under [newman](https://github.com/postmanlabs/newman) too:

```
newman run docs/postman/ryuk.postman_collection.json \
  -e docs/postman/ryuk.postman_environment.json
```

The **Errors** folder is meant to return 400. It is there to document what gets
rejected and why, so a full run is not expected to be green.
