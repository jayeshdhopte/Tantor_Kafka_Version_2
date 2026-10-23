import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { formatIstDateTime, formatLogTimestampsIst } from '../src/utils/istLogTime.ts';

test('66: UTC instants display in IST with an explicit label', () => {
  assert.equal(formatIstDateTime('2026-10-23T06:00:00Z'), '23/10/2026 | 11:30:00 IST');
  assert.equal(formatLogTimestampsIst('[2026-10-23T06:00:00Z] Started'), '[2026-10-23 11:30:00 IST] Started');
});

test('66: offsets represent the same instant and existing IST is never shifted twice', () => {
  for (const value of ['2026-10-23T06:00:00Z', '2026-10-23T11:30:00+05:30', '2026-10-23T11:30:00+0530', '2026-10-23T01:00:00-05:00']) {
    assert.equal(formatIstDateTime(value), '23/10/2026 | 11:30:00 IST');
  }
  const formatted = formatLogTimestampsIst('[2026-10-23T06:00:00Z] Done');
  assert.equal(formatLogTimestampsIst(formatted), formatted);
});

test('66: date, month, leap-day and year rollover are correct', () => {
  assert.equal(formatIstDateTime('2026-12-31T20:00:00Z'), '01/01/2027 | 01:30:00 IST');
  assert.equal(formatIstDateTime('2026-10-31T20:00:00Z'), '01/11/2026 | 01:30:00 IST');
  assert.equal(formatIstDateTime('2024-02-28T20:00:00Z'), '29/02/2024 | 01:30:00 IST');
  assert.equal(formatIstDateTime('2026-10-23T18:30:00Z'), '24/10/2026 | 00:00:00 IST');
});

test('66: Java nanoseconds, bare prefixes, levels and line endings are preserved', () => {
  const text = '[2026-10-23T06:00:00.123456789Z] Running\r\n2026-10-23T06:01:00Z Done\n[INFO] [2026-10-23T06:02:00Z] Healthy\n';
  assert.equal(formatLogTimestampsIst(text), '[2026-10-23 11:30:00.123456789 IST] Running\r\n2026-10-23 11:31:00 IST Done\n[INFO] [2026-10-23 11:32:00 IST] Healthy\n');
  assert.equal(formatLogTimestampsIst('[2026-10-23T06:01:00Z] [2026-10-23T06:00:00Z] Agent output'), '[2026-10-23 11:31:00 IST] [2026-10-23 11:30:00 IST] Agent output');
});

test('66: missing, ambiguous and invalid timestamps are not guessed or normalized', () => {
  for (const value of ['', 'invalid', '2026-10-23T06:00:00', '2026-02-30T06:00:00Z', '2026-10-23T24:00:00Z']) {
    assert.equal(formatIstDateTime(value), '-');
    assert.equal(formatLogTimestampsIst(`[${value}] message`), `[${value}] message`);
  }
  assert.equal(formatIstDateTime(null, 'N/A'), 'N/A');
  assert.equal(formatIstDateTime(undefined), '-');
});

test('66: message bodies, configuration, JSON and backup paths stay unchanged', () => {
  const lines = 'Saved /backup/2026-10-23T06:00:00Z/file\nPayload: {"time":"2026-10-23T06:00:00Z"}\nDeadline is 2026-10-23T06:00:00Z\nNo timestamp here';
  assert.equal(formatLogTimestampsIst(lines), lines);
  assert.equal(formatLogTimestampsIst(''), '');
});

test('66: results do not depend on the operating system or browser timezone', () => {
  const utility = new URL('../src/utils/istLogTime.ts', import.meta.url).href;
  const script = `import { formatIstDateTime } from ${JSON.stringify(utility)}; process.stdout.write(formatIstDateTime('2026-10-23T06:00:00Z'));`;
  for (const TZ of ['UTC', 'America/Los_Angeles', 'Asia/Kolkata']) {
    assert.equal(execFileSync(process.execPath, ['--input-type=module', '-e', script], { env: { ...process.env, TZ }, encoding: 'utf8' }), '23/10/2026 | 11:30:00 IST');
  }
});

test('66: screens, clipboard and download share formatting while backend storage stays UTC', () => {
  const job = readFileSync(new URL('../src/pages/JobStatusPage.tsx', import.meta.url), 'utf8');
  const deployment = readFileSync(new URL('../src/pages/DeploymentLogs.tsx', import.meta.url), 'utf8');
  const list = readFileSync(new URL('../src/pages/JobsList.tsx', import.meta.url), 'utf8');
  const server = readFileSync(new URL('../../tantor-server/src/main/java/io/translab/tantor/server/service/JobService.java', import.meta.url), 'utf8');
  assert.match(job, /formatLogTimestampsIst\(line\)\.replace/);
  assert.match(job, /formatIstDateTime\(job.startTime\)/);
  assert.match(deployment, /clipboard.writeText\(formatLogTimestampsIst/);
  assert.match(deployment, /const content = formatLogTimestampsIst/);
  assert.match(deployment, /formatIstDateTime\(selectedTask.createdAt\)/);
  assert.match(deployment, /formatIstDateTime\(selectedTask.updatedAt\)/);
  assert.match(list, /formatIstDateTime\(job.startTime/);
  assert.match(server, /job.setLogs\(currentLogs \+ "\[" \+ Instant.now\(\)/);
  assert.match(server, /step.setLogs\(current \+ "\[" \+ Instant.now\(\)/);
});
