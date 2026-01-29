import { execSync } from 'child_process';

interface TestResult {
  name: string;
  passed: boolean;
  duration: number;
}

const tests = [
  { name: 'Load Test V2', script: 'test:load' },
  { name: 'Reliability Chaos Test', script: 'test:reliability' },
  { name: 'Ordering Test', script: 'test:ordering' },
  { name: 'Ordering Gap Timeout Test', script: 'test:ordering-gap' },
  { name: 'DLQ Test', script: 'test:dlq' },
  { name: 'Idempotency Test V2', script: 'test:idempotency' },
  { name: 'Idempotency Key Test', script: 'test:idempotency-key' },
  { name: 'Idempotency Key Concurrency Test', script: 'test:idempotency-key-concurrency' },
  { name: 'Idempotency Key Mismatch Test', script: 'test:idempotency-key-mismatch' },
  { name: 'Rate Limit V2 Test', script: 'test:rate-limit' },
  { name: 'Multi-Consumer Test', script: 'test:multi-consumer' },
  { name: 'Backpressure Test', script: 'test:backpressure' },
  { name: 'Validation Test', script: 'test:validation' },
  { name: 'Custom Tests', script: 'test:custom' },
];

function runTest(name: string, script: string): TestResult {
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Running: ${name}`);
  console.log('─'.repeat(60));

  const startTime = Date.now();

  try {
    execSync(`npm run ${script}`, {
      encoding: 'utf8',
      timeout: 240000,
      stdio: 'inherit',
    });
    const duration = Date.now() - startTime;
    return { name, passed: true, duration };
  } catch {
    const duration = Date.now() - startTime;
    return { name, passed: false, duration };
  }
}

function calculateScore(results: TestResult[]): {
  score: number;
  grade: string;
  passedCount: number;
  totalCount: number;
} {
  const passedCount = results.filter((r) => r.passed).length;
  const totalCount = results.length;
  const score = Math.round((passedCount / totalCount) * 100);

  let grade: string;
  if (score === 100) grade = 'EXCELLENT';
  else if (score >= 80) grade = 'GOOD';
  else if (score >= 60) grade = 'PASS';
  else grade = 'FAIL';

  return { score, grade, passedCount, totalCount };
}

async function runSubmissionTest(): Promise<void> {
  console.log('\n' + '═'.repeat(60));
  console.log('SUBMISSION TEST - HARD V2.3');
  console.log('═'.repeat(60));
  console.log('\nThis will run each test sequentially and verify results.\n');

  const results: TestResult[] = [];

  for (const test of tests) {
    const result = runTest(test.name, test.script);
    results.push(result);
    await new Promise((r) => setTimeout(r, 2000));
  }

  const { score, grade, passedCount, totalCount } = calculateScore(results);

  console.log('\n' + '═'.repeat(60));
  console.log('SUBMISSION RESULTS');
  console.log('═'.repeat(60));

  console.log('\n  Individual Tests:\n');
  for (const result of results) {
    const status = result.passed ? 'PASS' : 'FAIL';
    const duration = `${(result.duration / 1000).toFixed(1)}s`;
    console.log(`    ${status}  ${result.name} (${duration})`);
  }

  console.log('\n' + '─'.repeat(60));
  console.log(`\n  Tests Passed:    ${passedCount}/${totalCount}`);
  console.log(`  Score:           ${score}/100`);
  console.log(`  Grade:           ${grade}`);

  console.log('\n' + '═'.repeat(60));

  if (grade === 'FAIL') {
    console.log('\nSubmission does not meet minimum requirements.');
    process.exit(1);
  } else {
    console.log('\nSubmission meets requirements.');
    process.exit(0);
  }
}

runSubmissionTest().catch((error) => {
  console.error('Submission test failed with error:', error);
  process.exit(1);
});
