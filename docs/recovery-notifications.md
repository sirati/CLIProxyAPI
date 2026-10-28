# Provider recovery notifications

CLI clients can carry an opaque recovery ID from a failed model request to an authenticated management connection. T3 Code can use that connection to wait for the requested route to become schedulable without retrying the conversation. The proxy exposes both a JSON status endpoint and a server-sent event subscription.

## Enable the integration

Set this in the v8 configuration:

```yaml
routing:
  recovery-notifications: true
```

The default is `false`. This integration currently supports locally managed routing. Home-managed instances do not create tickets or expose this endpoint because the control plane owns their availability state. Existing management access controls apply, including the remote access setting. No routes were added under `/v0/management`.

When the final failure is a retryable route-selection error, quota limit, or eligible upstream availability error, and no matching credential is schedulable, the proxy adds this marker to the CLI-visible error message:

```text
[cliproxy-recovery:K5wVx3M9Sq1zAx7B]
```

Extract it with `/\[cliproxy-recovery:([A-Za-z0-9_-]{16})\]/`. IDs contain 96 random bits encoded as URL-safe base64 without padding. They contain no account identity and are not management credentials. They survive Claude quota-message wrapping and Codex usage-limit conversion, including the Codex WebSocket terminal-error response. Existing reset times remain in the error for clients that have not implemented this integration.

An ID identifies the provider set and model of the failed route. It does not bind to the credential that happened to fail. Any matching credential becoming schedulable can release the wait. Invalid requests, permanent authentication failures, and failures where another matching credential is already schedulable do not receive tickets. IDs do not reserve credentials or guarantee that the next upstream request will succeed.

## Poll current availability

```http
GET /v8/management/routing/recovery/K5wVx3M9Sq1zAx7B
Authorization: Bearer <management-key>
```

A waiting response is HTTP 200 with `Cache-Control: no-store`:

```json
{
  "id": "K5wVx3M9Sq1zAx7B",
  "state": "waiting",
  "expires_at": "2026-10-29T07:00:00Z",
  "retry_at": "2026-10-28T09:00:00Z"
}
```

`retry_at` is an optional scheduling hint. A routing stop, missing credential, or authentication problem may have no known recovery time. A later check returns `"state": "available"` when a credential that supports this model passes the local routing availability checks. These checks do not execute a provider request or consume quota.

Unknown IDs, expired IDs, and a disabled integration return HTTP 404. Missing or invalid management authentication follows the existing management API's 401/403 behavior. A model API key is insufficient.

## Subscribe for recovery

Use the same URL with `?watch=true` and keep the response open:

```http
GET /v8/management/routing/recovery/K5wVx3M9Sq1zAx7B?watch=true
Authorization: Bearer <management-key>
Accept: text/event-stream
```

The response has `Content-Type: text/event-stream`. It immediately sends current status:

```text
event: waiting
data: {"id":"K5wVx3M9Sq1zAx7B","state":"waiting","expires_at":"2026-10-29T07:00:00Z"}

```

The proxy checks local availability every second. A `waiting` heartbeat is sent every 15 seconds. As soon as a check finds a schedulable credential, it sends `available` with the same JSON status shape, then closes the stream. If the ticket expires during the subscription, it sends `expired` with `{"id":"..."}` and closes. Disconnecting cancels the subscription. No callback URL is accepted or contacted.

A new subscription rechecks current state, so recovery between the original error and registration is not missed. If already available, the first event is `available` and the stream closes. Reconnecting does not require an event cursor. Availability is current state, not a durable historical event: a route can recover and become unavailable again before a client reconnects.

Tickets live for 24 hours, are process-local, and are lost on restart. At most 4096 tickets are retained per process. When the ticket capacity is exhausted, errors retain their ordinary reset/retry information without a new ID. In a multi-instance deployment, send status and subscription requests to the instance that issued the error. On 404, restart, or transport failure, retain the existing timed-resume behavior as a fallback. An available route can become busy again before a resumed request reaches it.

## T3 Code integration

The inspected T3 checkout already has `UsageLimitSourceConfig.url` and `managementKey`. Its current management client is `apps/server/src/usage/cliproxyApi.ts`; it builds `/v0/management` URLs and gives ordinary requests a 15-second timeout. Build this operation against `/v8/management` and use a separate long-lived request for the subscription. Do not apply the ordinary request timeout to it. Use server-side `fetch` with a bearer header and an `AbortSignal`; browser `EventSource` cannot attach this bearer header.

On a usage-limit or availability failure:

1. Extract the marker from the provider CLI's error text. Claude adapter handling lives in `apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts`; Codex usage-limit normalization lives in `apps/server/src/provider/codexUsageLimits.ts`.
2. Match the failure to the configured CLIProxyAPI source. The opaque ID cannot identify the hub URL. If several hubs are configured, authenticated JSON lookups can discover which one recognizes it.
3. Subscribe using that source's management key. Keep the existing reset-based resume as a fallback, with a shared guard so an event and a timer cannot resume the same paused turn twice.
4. On `available`, verify that the thread is still paused for that failure and resume it once. Do not resume canceled, replaced, or manually continued turns. Cancel the subscription when its paused turn is no longer current.
5. On `expired` or HTTP 404, use the existing fallback. On a temporary connection failure, reconnect the subscription without retrying model execution. Store the source and ID with paused-turn state if waits need to survive a T3 server restart.

The T3 checkout has not been modified as part of the proxy implementation.
