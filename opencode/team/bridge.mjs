import { createServer } from "node:http";
import { timingSafeEqual, randomBytes, createHash } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { NativeClient, NativeRequestError, nativeIdentity } from "./native-client.mjs";

const methods = Object.freeze({ team_send: "send", team_status: "status", team_update_progress: "updateProgress", team_complete_task: "completeTask", team_artifact_write: "artifactWrite", team_artifact_read: "artifactRead", team_artifact_list: "artifactList", team_artifact_mkdir: "artifactMkdir", team_artifact_preview: "artifactPreview" });
const equal = (a, b) => typeof a === "string" && Buffer.byteLength(a) === Buffer.byteLength(b) && timingSafeEqual(Buffer.from(a), Buffer.from(b));

// One bridge belongs to one supervised member process. Protocol/state semantics
// come from the same source as the existing Team adapter.
export async function createBridge({ protocol, native, environment = process.env, log = console }) {
 const cfg = protocol.readChannelConfig({});
 const runtime = protocol.createRuntime({ config: {}, logger: log });
 const generation = Number(environment.CLAWMANAGER_GATEWAY_GENERATION);
 // Pro's initial instance generation is zero; Lite Gateway generations start
 // at one. Keep the original generation for governance and fencing.
 const managedPro = environment.CLAWMANAGER_AGENT_ENABLED === "true" && /^[1-9][0-9]*$/.test(environment.INSTANCE_ID || "");
 if (!cfg.enabled || !cfg.teamId || !cfg.memberId || !Number.isSafeInteger(generation) || generation < 0 || generation === 0 && !managedPro) throw new Error("OpenCode Team identity unavailable");
 const token = randomBytes(32).toString("hex");
 let active = null, stopped = false, nativeGone = false, consumer;
 const abort = new AbortController();
 let stopInFlight, closeInFlight;
 async function stopExecution() {
  if (stopInFlight) return stopInFlight;
  if (!active) return;
  const current = active;
  stopInFlight = native.interrupt(current.identity).catch(error => { if (!nativeGone) throw error; }).then(() => { current.interrupted = true; }).finally(() => { stopInFlight = null; });
  return stopInFlight;
 }
 const server = createServer(async (request, response) => {
  const reply = (status, value) => { response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" }); response.end(JSON.stringify(value)); };
  if (!equal(request.headers.authorization, `Bearer ${token}`)) return reply(403, { error: "Team tool authorization failed" });
  if (request.method !== "POST" || request.url !== "/tool") return reply(404, { error: "Unknown Team operation" });
  try {
   let size = 0; const chunks = [];
   for await (const chunk of request) { size += chunk.length; if (size > 1024 * 1024) { reply(413, { error: "Team tool request too large" }); return; } chunks.push(chunk); }
   const input = JSON.parse(Buffer.concat(chunks));
   const current = active;
   if (!current || current.interrupted || stopped || input.sessionID !== current.ids.session || input.taskID !== current.ids.message || input.generation !== generation || !methods[input.name]) return reply(409, { error: "Team execution identity is not active" });
   if (!input.arguments || typeof input.arguments !== "object" || Array.isArray(input.arguments)) return reply(422, { error: "Invalid Team tool arguments" });
   if (typeof input.callID !== "string" || !input.callID || input.callID.length > 512) return reply(422, { error: "Missing native tool call identity" });
   const method = methods[input.name];
   const digest = createHash("sha256").update(JSON.stringify([input.name, input.arguments])).digest("hex");
   let call = current.calls.get(input.callID);
   if (call && call.digest !== digest) return reply(409, { error: "Conflicting native tool replay" });
   if (!call) {
    if (current.calls.size >= 1024) return reply(409, { error: "Native tool call limit reached" });
    call = { digest, result: Promise.resolve().then(() => runtime[method](method === "status" ? input.arguments.memberId : input.arguments)) };
    // Retain both successful and uncertain outcomes. A lost HTTP reply must not
    // execute the same native tool call twice. Cross-process business retries
    // remain protected by the authoritative Team protocol's durable receipts.
    current.calls.set(input.callID, call);
   }
   const value = await call.result;
   reply(200, { ok: true, result: value });
  } catch {
   // Runtime/model secrets and source URLs are never returned in tool errors.
   reply(500, { error: "Team tool failed; inspect the current work before retrying" });
  }
 });
 await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });

 async function consume(envelope) {
  if (stopped) throw new Error("OpenCode Team bridge is stopping");
  if (await protocol.workflowReminderIsStale(cfg, envelope, log)) return;
  const identity = { teamId: String(cfg.teamId), memberId: cfg.memberId, taskId: String(envelope.rootTaskId || envelope.taskId || ""), messageId: String(envelope.messageId || ""), generation };
  const ids = nativeIdentity(identity);
  if (active) throw new Error("OpenCode member already owns an execution");
  const current = { identity, ids, interrupted: false, calls: new Map() };
  active = current;
  const startedAt = Date.now();
  let activity, stopHeartbeat = () => {};
  const emitTaskEvent = async (event, extra) => {
   if (event === "assignment_heartbeat" && runtime.isActiveTaskCompleted(envelope.taskId)) return;
   const redis = new protocol.RedisClient(cfg.redisUrl);
   try {
    await redis.connect();
    await protocol.xaddJson(redis, protocol.eventsKey(cfg), protocol.taskEvent(cfg, event, envelope, extra));
   } finally { redis.close(); }
  };
  try {
   let result = await runtime.withActiveEnvelope(envelope, async () => {
    if (!protocol.isContextOnlyEnvelope(envelope)) {
     await protocol.writeLocalStatus(cfg, { availability: "busy", runtimeStatus: "running", currentTaskId: identity.taskId, currentAssignmentId: envelope.assignmentId || envelope.workId, currentWorkId: envelope.workId || envelope.assignmentId, currentSourceMessageId: envelope.messageId, currentRevision: Math.max(1, Number(envelope.revision) || 1), lastSummary: "OpenCode Team task started" });
     await emitTaskEvent("task_started", { availability: "busy", runtimeStatus: "running", summary: "OpenCode Team task started" });
     stopHeartbeat = protocol.startAssignmentHeartbeat({ envelope, emitTaskEvent, log, isTerminal: () => runtime.isActiveTaskCompleted(envelope.taskId) });
     activity = await protocol.startAssignmentActivityObserver({ cfg, envelope, startedAt, log, observe: () => current.observed });
    }
    const text = protocol.appendRedisTeamCompletionGuidance(await protocol.appendLeaderTeamContext(String(envelope.text || ""), cfg, envelope), envelope);
    await native.admit(identity, environment.CLAWMANAGER_DEFAULT_PROJECT_PATH || environment.CLAWMANAGER_PROJECT_PATH, [environment.CLAWMANAGER_TEAM_SYSTEM_PROMPT || "", text].filter(Boolean).join("\n\n"), abort.signal);
    const deadline = Date.now() + cfg.embeddedTimeoutSeconds * 1000;
    let idle = 0, cursor = 0, ownsInput = false;
    while (!current.interrupted) {
     cursor = await native.readLog(identity, cursor, event => {
      if (event.type === "session.inbox.delivered") ownsInput = event.data.inboxID === ids.message;
      if (!ownsInput) return;
      // Reasoning content and tool arguments/results never leave the native
      // conversation. Only bounded execution facts inform Team supervision.
      const mtimeMs = Number(event.created);
      const previous = current.observed || {};
      const observed = { ...previous, mtimeMs, lastSessionEventAt: new Date(mtimeMs).toISOString(), sessionCursor: String(event.durable.seq) };
      if (event.type === "session.text.ended") Object.assign(observed, { lastActivityKind: "assistant", lastAssistantText: String(event.data.text || "").slice(0, 4000), lastAssistantAt: observed.lastSessionEventAt });
      else if (event.type === "session.tool.input.started") Object.assign(observed, { lastActivityKind: "tool_call", pendingToolName: String(event.data.name || "").slice(0, 120) });
      else if (["session.tool.success", "session.tool.failed"].includes(event.type)) Object.assign(observed, { lastActivityKind: "tool_result", lastToolName: previous.pendingToolName || "", pendingToolName: "", lastToolFailed: event.type.endsWith("failed"), lastToolAt: observed.lastSessionEventAt });
      else if (event.type === "session.reasoning.ended") observed.lastActivityKind = "model_output";
      current.observed = observed;
     }, abort.signal);
     const state = await native.api("/api/session/active", { signal: abort.signal });
     const inbox = await native.api(`/api/session/${ids.session}/inbox`, { signal: abort.signal });
     // Never mistake scheduling latency or an input parked in native inbox for
     // a finished turn. Native message persistence confirms input consumption.
     let consumed = false;
     try { await native.api(`/api/session/${ids.session}/message/${ids.message}`, { signal: abort.signal }); consumed = true; }
     catch (error) { if (!(error instanceof NativeRequestError) || error.status !== 404) throw error; }
     if (consumed && !state.data?.[ids.session] && Array.isArray(inbox.data) && !inbox.data.some(item => item.id === ids.message)) idle++; else idle = 0;
     if (idle >= 2) break;
     if (Date.now() >= deadline) { await stopExecution(); throw new Error("OpenCode Team execution timed out after confirmed interrupt"); }
     await sleep(1000, undefined, { signal: abort.signal });
    }
    // Native idle is an execution fact, never a business completion receipt.
   }, cfg);
   if (current.interrupted || stopped) throw new Error("OpenCode Team execution stopped; original work is retained");
   const terminal = await runtime.isTaskTerminal(cfg, envelope);
   if (!terminal) {
    const durable = await protocol.readTurnFacts(cfg, envelope);
    result = protocol.mergeActiveTurnFacts(result, durable);
    if (!result.completed && !result.completionPending) {
     const routing = await protocol.activeMemberRouting(cfg, result.outbound);
     const discovered = routing.currentIsLeader
      ? await protocol.collectRootTaskArtifactRefs(cfg, identity.taskId)
      : await protocol.collectMemberAssignmentArtifactRefs(cfg, identity.taskId, cfg.memberId, envelope.assignmentId || envelope.workId);
     const artifactRefs = await protocol.validateArtifactRefs(cfg, [...(result.artifactRefs || []), ...discovered]);
     const observation = protocol.observeTeamTurnOutcome({ envelope, activeResult: result, durableFacts: durable, terminalAfterDispatch: false, contextOnly: protocol.isContextOnlyEnvelope(envelope), memberRouting: routing });
     const progress = protocol.turnFinishedWithoutCompletionEvent(envelope, { fallbackText: current.observed?.lastAssistantText || "", artifactRefs, hadOutboundAssignment: routing.leaderCoordination, observation });
     await protocol.writeLocalStatus(cfg, { availability: progress.availability, runtimeStatus: progress.runtimeStatus, currentTaskId: identity.taskId, lastSummary: progress.summary });
     await emitTaskEvent("task_progress", progress);
    }
   }
  } finally {
   stopHeartbeat();
   await activity?.stop(current.interrupted ? "cancelled" : "turn_finished").catch(() => log.warn?.("OpenCode Team activity close remains unconfirmed"));
   // If the transport fails, keep identity bound until native execution is
   // proven idle. Do not allow a later delivery to steal its tools.
   if (!current.interrupted) {
    const state = await native.api("/api/session/active");
    if (state.data?.[ids.session]) await stopExecution();
   }
   if (active === current) active = null;
  }
 }
 return {
  port: server.address().port, token,
  async start() {
   consumer = await protocol.startConsumer(cfg, consume, async envelope => {
    log.error("OpenCode Team delivery failed; original work remains available for recovery");
    if (await runtime.isTaskTerminal(cfg, envelope)) return;
    await protocol.writeLocalStatus(cfg, { availability: "busy", runtimeStatus: "recovering", currentTaskId: envelope.taskId, currentAssignmentId: envelope.assignmentId || envelope.workId, lastSummary: "OpenCode Team runtime reconciliation is required" });
    const redis = new protocol.RedisClient(cfg.redisUrl);
    try {
     await redis.connect();
     await protocol.xaddJson(redis, protocol.eventsKey(cfg), protocol.taskEvent(cfg, "task_progress", envelope, { status: "running", availability: "busy", runtimeStatus: "recovering", eventKind: "runtime_reconciliation_needed", failureDomain: "runtime_adapter", retryable: true, stateEffect: "none", nonAuthoritative: true, rootTaskTerminal: false, visibleToChat: false, chatPolicy: "hidden", summary: "OpenCode Team runtime reconciliation is required" }));
    } finally { redis.close(); }
   }, log);
   if (!consumer) throw new Error("OpenCode Team consumer did not start");
  },
  async stop({ nativeExited = false } = {}) {
   if (nativeExited) nativeGone = true;
   if (nativeExited && active) active.interrupted = true;
   if (closeInFlight) return closeInFlight;
   stopped = true;
   closeInFlight = (async () => {
    // Stop admission first. The accepted native execution is then interrupted
    // and joined; untouched inbox entries remain pending for the next process.
    await consumer?.stop({ waitForExecution: false });
    if (nativeExited && active) active.interrupted = true;
    else await stopExecution();
    abort.abort();
    await consumer?.stop();
    await new Promise(resolve => server.close(resolve));
   })().catch(error => { closeInFlight = null; throw error; });
   return closeInFlight;
  },
 };
}

export { NativeClient };
