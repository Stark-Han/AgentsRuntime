import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

const patchScript = path.resolve(import.meta.dirname, "patch_browser_proxy_navigation.mjs");
const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-browser-proxy-patch-"));
try {
  const dist = path.join(fixtureRoot, "dist");
  fs.mkdirSync(dist, { recursive: true });
  fs.writeFileSync(path.join(fixtureRoot, "package.json"), JSON.stringify({ version: "2026.8.1", type: "module" }));
  fs.writeFileSync(path.join(dist, "chrome-fixture.js"), [
    "const lookups = [];",
    "function normalizeHostname(value) { return String(value).toLowerCase(); }",
    "function isPrivateNetworkAllowedByPolicy(policy) { return policy?.dangerouslyAllowPrivateNetwork === true; }",
    "async function resolvePinnedHostnameWithPolicy(hostname) { lookups.push(hostname); }",
    "async function assertBrowserNavigationAllowed(opts) {",
    "\tconst parsed = new URL(opts.url);",
    "\tawait resolvePinnedHostnameWithPolicy(parsed.hostname, {",
    "\t\tpolicy: opts.ssrfPolicy",
    "\t});",
    "}",
    "export async function check(url, browserProxyMode, allowPrivate = true) { const before = lookups.length; await assertBrowserNavigationAllowed({ url, browserProxyMode, ssrfPolicy: { dangerouslyAllowPrivateNetwork: allowPrivate } }); return lookups.length - before; }",
  ].join("\n"));
  fs.writeFileSync(path.join(dist, "routes-fixture.js"), [
    "function withBrowserNavigationPolicy(ssrfPolicy, extra) { return { ssrfPolicy, ...extra }; }",
    "function resolveBrowserNavigationProxyMode() { return 'explicit-browser-proxy'; }",
    "function browserNavigationPolicyForProfile(ctx, profileCtx) {",
    "\treturn withBrowserNavigationPolicy(ctx.state().resolved.ssrfPolicy, { browserProxyMode: resolveBrowserNavigationProxyMode({",
    "\t\tresolved: ctx.state().resolved,",
    "\t\tprofile: profileCtx.profile",
    "\t}) });",
    "}",
    "async function fixtures(ctx, profileCtx, pw, cdpUrl, tab, plan) {",
    "\t\t\t\t\t\tconst snap = await pw.snapshotRoleViaPlaywright({",
    "\t\t\t\t\t\t\tcdpUrl,",
    "\t\t\t\t\t\t\ttargetId: tab.targetId,",
    "\t\t\t\t\t\t\tssrfPolicy: ctx.state().resolved.ssrfPolicy",
    "\t\t\t\t\t\t});",
    "\t\t\t\t\t\tconst roleSnapshotArgs = {",
    "\t\t\t\t\t\t\trefsMode: plan.refsMode,",
    "\t\t\t\t\t\t\tssrfPolicy: ctx.state().resolved.ssrfPolicy,",
    "\t\t\t\t\t\t\turls: plan.urls,",
    "\t\t\t\t\t\t};",
    "\t\t\t\t\t\tawait pw.snapshotAiViaPlaywright({",
    "\t\t\t\t\t\t\ttargetId: tab.targetId,",
    "\t\t\t\t\t\t\tssrfPolicy: ctx.state().resolved.ssrfPolicy,",
    "\t\t\t\t\t\t\turls: plan.urls,",
    "\t\t\t\t\t\t});",
    "\t\t\t\t\t\tawait pw.snapshotAriaViaPlaywright({",
    "\t\t\t\t\t\t\t\ttimeoutMs: plan.timeoutMs,",
    "\t\t\t\t\t\t\t\tssrfPolicy: ctx.state().resolved.ssrfPolicy",
    "\t\t\t\t\t\t});",
    "\treturn { snap, roleSnapshotArgs };",
    "}",
  ].join("\n"));
  fs.writeFileSync(path.join(dist, "pw-ai-fixture.js"), [
    "const observed = [];",
    "async function getPageForTargetId(opts) { return { url: () => opts.url }; }",
    "function ensurePageState() {}",
    "async function assertPageNavigationCompletedSafely(opts) { observed.push(opts.browserProxyMode); }",
    "async function prepareSnapshotPageViaPlaywright(opts) {",
    "\tconst page = await getPageForTargetId({ cdpUrl: opts.cdpUrl, targetId: opts.targetId, url: opts.url });",
    "\tensurePageState(page);",
    "\tif (opts.ssrfPolicy) await assertPageNavigationCompletedSafely({",
    "\t\tcdpUrl: opts.cdpUrl,",
    "\t\tpage,",
    "\t\tresponse: null,",
    "\t\tssrfPolicy: opts.ssrfPolicy,",
    "\t\ttargetId: opts.targetId",
    "\t});",
    "\treturn page;",
    "}",
    "async function snapshotAriaViaPlaywright(opts) { await prepareSnapshotPageViaPlaywright({",
    "\t\tssrfPolicy: opts.ssrfPolicy",
    "\t}); return observed.at(-1); }",
    "async function snapshotAiViaPlaywright(opts) { await prepareSnapshotPageViaPlaywright({",
    "\t\tssrfPolicy: opts.ssrfPolicy",
    "\t}); return observed.at(-1); }",
    "async function snapshotRoleViaPlaywright(opts) { await prepareSnapshotPageViaPlaywright({",
    "\t\tssrfPolicy: opts.ssrfPolicy",
    "\t}); return observed.at(-1); }",
    "async function navigateViaPlaywright(opts) { return opts; }",
    "export { snapshotAriaViaPlaywright, snapshotAiViaPlaywright, snapshotRoleViaPlaywright };",
  ].join("\n"));

  const run = (mode) => spawnSync(process.execPath, [patchScript, mode], { env: { ...process.env, OPENCLAW_PACKAGE_ROOT: fixtureRoot }, encoding: "utf8" });
  const patched = run("--patch");
  assert.equal(patched.status, 0, patched.stderr || patched.stdout);
  const verified = run("--verify");
  assert.equal(verified.status, 0, verified.stderr || verified.stdout);

  const chrome = await import(pathToFileURL(path.join(dist, "chrome-fixture.js")).href);
  const preview = "http://p-abcdefghijklmnop.clawmanager-team-preview.invalid/path";
  assert.equal(await chrome.check(preview, "explicit-browser-proxy"), 0);
  assert.equal(await chrome.check(preview, "direct"), 1);
  assert.equal(await chrome.check(preview, "explicit-browser-proxy", false), 1);
  assert.equal(await chrome.check("https://example.com", "explicit-browser-proxy"), 1);

  const pw = await import(pathToFileURL(path.join(dist, "pw-ai-fixture.js")).href);
  const opts = { ssrfPolicy: {}, browserProxyMode: "explicit-browser-proxy" };
  assert.equal(await pw.snapshotAiViaPlaywright(opts), "explicit-browser-proxy");
  assert.equal(await pw.snapshotRoleViaPlaywright(opts), "explicit-browser-proxy");
  assert.equal(await pw.snapshotAriaViaPlaywright(opts), "explicit-browser-proxy");

  const routeSource = fs.readFileSync(path.join(dist, "routes-fixture.js"), "utf8");
  assert.equal((routeSource.match(/\.\.\.browserNavigationPolicyForProfile\(ctx, profileCtx\)/g) || []).length, 4);
  assert.ok(!routeSource.includes("__clawmanagerBrowserProxyMode"));
} finally {
  fs.rmSync(fixtureRoot, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}

process.stdout.write("OpenClaw 8.1 native Browser proxy navigation patch test passed\n");
