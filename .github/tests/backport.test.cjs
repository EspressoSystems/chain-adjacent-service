const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { resolve } = require('node:path');
const { test } = require('node:test');
const { runInNewContext } = require('node:vm');

// Compile the actual inline workflow script, not a separately maintained copy.
const workflow = readFileSync(resolve(__dirname, '../workflows/backport.yml'), 'utf8');
const script = workflow.split('          script: |\n')[1]
  .split('\n').map(line => line.replace(/^ {12}/, '')).join('\n');

async function report({ results = '', success = '', outcome = 'success', issues = [] } = {}) {
  const calls = [];
  const github = {
    paginate: async () => issues,
    rest: { issues: {
      listForRepo() {},
      create: async args => calls.push({ method: 'create', ...args }),
      update: async args => calls.push({ method: 'update', ...args }),
    } },
  };
  await runInNewContext('(async () => {\n' + script + '\n})()', {
    github,
    context: {
      payload: { pull_request: { number: 98, html_url: 'https://github.com/owner/repo/pull/98' } },
      repo: { owner: 'owner', repo: 'repo' },
      serverUrl: 'https://github.com', runId: 123,
    },
    process: { env: {
      BACKPORT_RESULTS: results, BACKPORT_SUCCESS: success, BACKPORT_OUTCOME: outcome,
      GITHUB_RUN_ATTEMPT: '2',
    } },
  });
  return calls;
}

test('no target labels: compiles and performs no writes', async () => {
  assert.deepEqual(await report(), []);
});
test('all targets succeeded: performs no writes', async () => {
  assert.deepEqual(await report({ results: 'release/a=true\nrelease/b=true\n', success: 'true' }), []);
});
test('newline-separated mixed results produce one useful failure issue', async () => {
  const [issue] = await report({ results: 'release/a=false\nrelease/b=true\n', success: 'false' });
  assert.equal(issue.method, 'create');
  assert.match(issue.body, /Failed targets: release\/a/);
  assert.match(issue.body, /Successful targets: release\/b/);
  assert.match(issue.body, /actions\/runs\/123\/attempts\/2/);
  assert.equal(issue.labels, undefined);
  assert.equal(issue.assignees, undefined);
});
test('branch quotes, backticks and equals remain data', async () => {
  const branch = "release/'quoted`=${literal}";
  const [issue] = await report({ results: branch + '=false\r\nnext=true\n' });
  assert.ok(issue.body.includes('Failed targets: ' + branch));
});
test('action error without outputs still produces a diagnostic issue', async () => {
  const [issue] = await report({ outcome: 'failure' });
  assert.match(issue.body, /Target results may be incomplete/);
  assert.match(issue.body, /Not reported; see workflow logs/);
});
test('aggregate failure without target details is not silently ignored', async () => {
  assert.equal((await report({ success: 'false' })).length, 1);
});
test('reruns update an existing open report rather than duplicate it', async () => {
  const [issue] = await report({ results: 'main=false', issues: [
    { number: 1, body: '<!-- backport-failure:97 -->' },
    { number: 2, body: '<!-- backport-failure:98 -->', pull_request: {} },
    { number: 3, body: '<!-- backport-failure:98 -->\nPrevious report' },
  ] });
  assert.equal(issue.method, 'update');
  assert.equal(issue.issue_number, 3);
});
test('invalid results fail explicitly instead of dropping a target', async () => {
  await assert.rejects(report({ results: 'release/a=unknown' }), /Invalid backport result/);
});
test('workflow passes outputs as data and reports even after action failure', () => {
  assert.ok(!script.includes('${{'));
  assert.match(workflow, /BACKPORT_RESULTS: \$\{\{ steps.backport.outputs.was_successful_by_target/);
  assert.match(workflow, /!cancelled\(\) && \(steps.backport.outcome == 'success' \|\| steps.backport.outcome == 'failure'\)/);
  assert.match(workflow, /cancel-in-progress: false/);
});
