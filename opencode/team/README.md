# OpenCode Team adapter

The governed OpenCode 2.0.27 delivery image supports Team Leaders and Workers.
The Lite and Pro images use the same native tools and Redis protocol.

`start-opencode-web` starts the member supervisor only when
`CLAWMANAGER_TEAM_ENABLED=true`. Ordinary instances keep the native serve path.
The final image must advertise `CLAWMANAGER_REDIS_TEAM_PLUGIN_VERSION=opencode-team-v1`;
an unpatched image must not advertise this capability. ClawManager excludes
incompatible pools when scheduling OpenCode Team members.

The image packages the authoritative Redis Team implementation from
`plugins/openclaw-redis-team/dist/index.js`, without its OpenClaw host entry or
SDK import. It does not create a second Team state machine. The native v2 tools
are installed by `../model-governance/apply-governance.mjs`.

Each accepted transport delivery maps deterministically to a native root-task
session and input message. Redis deduplication, native input admission and the
existing business receipts protect replay. The local tool bridge checks the
authenticated native session, input and process generation; models do not need
to provide these fields. Same-process retries retain the native tool call result.
After a restart, the shared protocol's durable business receipts remain the
authority; an uncertain external operation is not declared successful.

Presence and assignment monitoring run independently of a model turn. The
supervisor reads the bounded durable native event log for execution evidence;
reasoning content and raw tool arguments/results are not projected. A deadline
interrupts the accepted native execution and checks actual idle before another
delivery can own the member. Unknown stop retains the current binding. Shutdown
stops inbox admission first and leaves unprocessed messages pending.

A native idle state, prose or file is never a completion receipt. Missing
completion is reported through the existing turn-outcome recovery protocol.
Worker delivery cannot close the Leader root. Shared files use the existing
member/task scope and preview service, including owner authorization.

Targeted checks:

```sh
node --test opencode/team/*.test.mjs
REDIS_TEAM_TEST_URL=redis://127.0.0.1:6379 node opencode/team/redis-integration.mjs
```

The Redis test uses controlled native execution and does not claim real-model
acceptance. Delivery also requires the native source typecheck/tool tests,
actual image checks and mixed/all-OpenCode Team acceptance against the target
ClawManager backend. Existing busy OpenCode instances must not be moved between
pools using the old destructive shared-pool replacement path.
