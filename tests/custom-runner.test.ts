import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

function findCustomTests(customDir: string): string[] {
  if (!fs.existsSync(customDir)) return [];
  return fs
    .readdirSync(customDir)
    .filter((f) => f.endsWith('.test.ts'))
    .map((f) => path.join(customDir, f));
}

function runTestFile(filePath: string): void {
  execSync(`node -r ts-node/register \"${filePath}\"`, {
    stdio: 'inherit',
  });
}

function runCustomTests(): TestResult {
  console.log('\n' + '═'.repeat(60));
  console.log('CUSTOM TESTS RUNNER V2.3');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];
  const rootDir = path.join(__dirname, '..');
  const customDir = path.join(__dirname, 'custom');

  if (!fs.existsSync(customDir)) {
    failureReasons.push('Missing tests/custom directory');
    return { passed: false, failureReasons };
  }

  const packageJsonPath = path.join(rootDir, 'package.json');
  if (!fs.existsSync(packageJsonPath)) {
    failureReasons.push('Missing package.json');
    return { passed: false, failureReasons };
  }

  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8')) as {
    scripts?: Record<string, string>;
  };

  if (!packageJson.scripts || !packageJson.scripts['test:custom']) {
    failureReasons.push('Missing npm script test:custom');
  }

  const testFiles = findCustomTests(customDir);
  if (testFiles.length < 3) {
    failureReasons.push(`Found ${testFiles.length} custom tests, need at least 3`);
  }

  if (failureReasons.length > 0) {
    return { passed: false, failureReasons };
  }

  console.log(`\nRunning ${testFiles.length} custom tests...`);
  for (const testFile of testFiles) {
    console.log(`\nRunning: ${path.relative(rootDir, testFile)}`);
    runTestFile(testFile);
  }

  return { passed: true, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('CUSTOM TESTS RUNNER V2.3 RESULTS');
  console.log('═'.repeat(60));

  if (result.passed) {
    console.log('PASSED');
  } else {
    console.log('FAILED');
    for (const reason of result.failureReasons) {
      console.log(`   - ${reason}`);
    }
  }
  console.log('═'.repeat(60) + '\n');
}

try {
  const result = runCustomTests();
  printResult(result);
  process.exit(result.passed ? 0 : 1);
} catch (error) {
  console.error('Custom tests failed with error:', error);
  process.exit(1);
}
