import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildProtocol } from "./build-protocol.mjs";

test("the delivery package imports the authoritative protocol without an OpenClaw SDK", async () => {
 const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-team-protocol-"));
 try {
  const output = path.join(temporary, "protocol.mjs");
  await buildProtocol(output);
  const protocol = await import(pathToFileURL(output));
  assert.equal(typeof protocol.createRuntime, "function");
  assert.equal(typeof protocol.startConsumer, "function");
  assert.equal(typeof protocol.RedisClient, "function");
  const source = await fs.readFile(output, "utf8");
  assert.ok(!source.includes('from "openclaw/plugin-sdk'));
  assert.ok(!source.includes("export default definePluginEntry"));
 } finally { await fs.rm(temporary, { recursive: true, force: true }); }
});
