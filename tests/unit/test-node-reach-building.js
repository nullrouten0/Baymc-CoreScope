/* test-node-reach-building.js
 *
 * The reach endpoint answers 202 {"status":"building"} while a big node is still
 * being computed. The page must keep asking (not show an error), tell the user it
 * is still working, stop if the user moves on, and give up after a few minutes.
 * Loads only the fetchReach helper out of public/node-reach.js.
 */
'use strict';
const fs = require('fs');
const assert = require('assert');

const src = fs.readFileSync(__dirname + '/../../public/node-reach.js', 'utf8');
const m = src.match(/\/\/ BEGIN fetchReach[^\n]*\n([\s\S]*?)\/\/ END fetchReach/);
assert(m, 'fetchReach markers not found in node-reach.js');
const make = new Function('Date', m[1] + '; return { fetchReach: fetchReach, setMax: function (v) { REACH_MAX_WAIT_MS = v; } };');

let passed = 0, failed = 0;
async function test(name, fn) {
  try { await fn(); passed++; console.log('  ✅ ' + name); }
  catch (e) { failed++; console.log('  ❌ ' + name + ': ' + (e && e.message || e)); }
}
const building = { status: 'building', retry_after_seconds: 5, detail: 'x' };
const report = { node: { pubkey: 'ab' }, links: [] };

(async () => {
  await test('keeps asking while the server says building, then returns the report', async () => {
    const { fetchReach } = make(Date);
    const replies = [building, building, report];
    let calls = 0; const waits = []; const sleeps = [];
    const d = await fetchReach(async () => replies[calls++], '/x', {
      isCurrent: () => true, sleep: async ms => { sleeps.push(ms); }, onWait: s => waits.push(s)
    });
    assert.strictEqual(d, report);
    assert.strictEqual(calls, 3);
    assert.strictEqual(waits.length, 2, 'told the user twice');
    assert.deepStrictEqual(sleeps, [5000, 5000], 'honours retry_after_seconds');
  });

  await test('a normal report comes straight back with no waiting', async () => {
    const { fetchReach } = make(Date);
    let slept = false;
    const d = await fetchReach(async () => report, '/x', { isCurrent: () => true, sleep: async () => { slept = true; }, onWait() {} });
    assert.strictEqual(d, report); assert.strictEqual(slept, false);
  });

  await test('a report that merely contains the word building is not mistaken for the placeholder', async () => {
    const { fetchReach } = make(Date);
    const real = { status: 'building', node: { pubkey: 'ab' }, links: [] };
    const d = await fetchReach(async () => real, '/x', { isCurrent: () => true, sleep: async () => {}, onWait() {} });
    assert.strictEqual(d, real);
  });

  await test('stops quietly when the user has moved to another node or range', async () => {
    const { fetchReach } = make(Date);
    let current = true, calls = 0;
    const d = await fetchReach(async () => { calls++; return building; }, '/x', {
      isCurrent: () => current, sleep: async () => { current = false; }, onWait() {}
    });
    assert.strictEqual(d, null); assert.strictEqual(calls, 1);
  });

  await test('gives up with a plain message after the maximum wait', async () => {
    const h = make(Date); h.setMax(-1);
    await assert.rejects(
      h.fetchReach(async () => building, '/x', { isCurrent: () => true, sleep: async () => {}, onWait() {} }),
      /taking too long/);
  });

  await test('an error from the server still surfaces (not swallowed by the retry loop)', async () => {
    const { fetchReach } = make(Date);
    await assert.rejects(
      fetchReach(async () => { throw new Error('API 500: /x'); }, '/x', { isCurrent: () => true, sleep: async () => {}, onWait() {} }),
      /API 500/);
  });

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
