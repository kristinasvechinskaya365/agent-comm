# REST API Reference

All endpoints return JSON. CORS enabled on all routes.

## Read endpoints

| Method | Path                            | Description                                             |
| ------ | ------------------------------- | ------------------------------------------------------- |
| GET    | `/health`                       | Server status, version, uptime, agent count             |
| GET    | `/api/agents`                   | List online agents                                      |
| GET    | `/api/agents/:id`               | Get agent by ID or name                                 |
| GET    | `/api/agents/:id/heartbeat`     | Agent heartbeat status (age, status, status_text)       |
| GET    | `/api/channels`                 | List active channels                                    |
| GET    | `/api/channels/:name`           | Channel details with members                            |
| GET    | `/api/channels/:name/members`   | Channel member list                                     |
| GET    | `/api/channels/:name/messages`  | Channel messages (`?limit=50`)                          |
| GET    | `/api/messages`                 | List messages (`?limit=50&from=&to=&offset=`)           |
| GET    | `/api/messages/:id/thread`      | Get full thread                                         |
| GET    | `/api/search`                   | Full-text search (`?q=keyword&limit=20&channel=&from=`) |
| GET    | `/api/state`                    | List present state entries (`?namespace=&prefix=`)      |
| GET    | `/api/state/:namespace/:key`    | Get a present state entry (legacy shape)                |
| GET    | `/api/state/v2/:namespace/:key` | Get generation, presence, and entry/tombstone           |
| GET    | `/api/feed`                     | Activity feed events (`?agent=&type=&since=&limit=50`)  |
| GET    | `/api/branches`                 | List branches (`?message_id=` to filter by parent)      |
| GET    | `/api/branches/:id`             | Get specific branch by ID                               |
| GET    | `/api/branches/:id/messages`    | Get messages in a branch                                |
| GET    | `/api/stuck`                    | Detect stuck agents (`?threshold_minutes=10`)           |
| GET    | `/api/overview`                 | Full snapshot (agents, channels, messages, state, feed) |
| GET    | `/api/export`                   | Full database export as JSON                            |

## Write endpoints

| Method | Path                                | Body                                                      | Description                            |
| ------ | ----------------------------------- | --------------------------------------------------------- | -------------------------------------- |
| POST   | `/api/messages`                     | `{from, to?, channel?, content, importance?, thread_id?}` | Send a message                         |
| POST   | `/api/state/:namespace/:key`        | `{value, updated_by, ttl_seconds?}`                       | Set state entry                        |
| POST   | `/api/state/:namespace/:key/cas`    | `{expected, new_value, updated_by, ttl_seconds?}`         | Legacy value compare-and-swap          |
| POST   | `/api/state/v2/:namespace/:key/cas` | See versioned state contract below                        | Compare generation and transition      |
| DELETE | `/api/messages`                     | —                                                         | Purge all messages                     |
| DELETE | `/api/messages`                     | `{before?, from?, channel?}`                              | Delete messages by filter              |
| DELETE | `/api/messages/:id`                 | `{agent_id}`                                              | Delete a message (sender only)         |
| DELETE | `/api/state/:namespace/:key`        | —                                                         | Delete state entry                     |
| DELETE | `/api/agents/offline`               | —                                                         | Purge offline agents                   |
| POST   | `/api/cleanup`                      | —                                                         | Trigger manual cleanup                 |
| POST   | `/api/cleanup/stale`                | —                                                         | Clean up stale agents and old messages |
| POST   | `/api/cleanup/full`                 | —                                                         | Full database cleanup                  |

## Versioned state (v2)

`GET /api/state/v2/:namespace/:key` always returns exactly:

```json
{ "generation": 0, "present": false, "entry": null }
```

Generation `0` means never seen. A successful mutation advances the server-owned safe-integer generation by exactly one. Deletes, TTL expiry, and cleanup retain an absent tombstone (`present: false`, `entry: null`) without returning the deleted value or owner. Legacy GET/list endpoints continue to return only present `StateEntry` objects.

Use `POST /api/state/v2/:namespace/:key/cas` with one unambiguous intent:

```json
{
  "expected_generation": 0,
  "operation": "set",
  "value": "opaque client value",
  "updated_by": "agent-id",
  "ttl_seconds": 60
}
```

or:

```json
{ "expected_generation": 1, "operation": "delete" }
```

The response is exactly `{ "swapped", "predecessor", "successor" }`. On success, `successor.generation = predecessor.generation + 1`. On mismatch, no CAS mutation occurs and both `predecessor` and `successor` are the exact current version. Lazy TTL expiry linearizes before the comparison, so a stale live generation observes the resulting tombstone mismatch. `expected_generation` must be an integer from `0` through `Number.MAX_SAFE_INTEGER`; advancing the maximum fails closed. Unknown fields and set-only fields on a delete intent are rejected. Values, owners, timestamps, TTLs, envelopes, and revision-like fields cannot set or substitute for generation.

MCP `comm_state` preserves the legacy actions and adds `get_v2` and `cas_v2`. `cas_v2` uses `expected_generation`, `operation`, `value?`, and `ttl_seconds?`; `updated_by` is always the registered MCP agent and cannot be supplied.

Library consumers use `StateService.getVersioned()` and `StateService.compareGeneration()`. `agent-comm/lib` exports `StateVersion`, `StateTransitionIntent`, `StateGenerationTransitionResult`, and `MAX_STATE_GENERATION`.

## Authentication

The REST API is **unauthenticated**. It is designed for localhost use between trusted agents. The only protection on write endpoints is:

- `POST /api/messages` requires the `from` agent to be online (prevents impersonation of offline agents)
- `DELETE /api/messages/:id` requires the `agent_id` to match the message sender

## Error responses

```json
{
  "error": "Description of the error",
  "code": "VALIDATION_ERROR"
}
```

| Status | Code               | Meaning                                     |
| ------ | ------------------ | ------------------------------------------- |
| 400    | —                  | Bad request (missing params, invalid input) |
| 404    | `NOT_FOUND`        | Entity not found                            |
| 409    | `CONFLICT`         | Conflict (e.g. duplicate agent name)        |
| 422    | `VALIDATION_ERROR` | Input validation failure                    |
| 429    | `RATE_LIMITED`     | Rate limit exceeded                         |
| 500    | —                  | Internal server error                       |
