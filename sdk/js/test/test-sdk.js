/**
 * Tests for the Nexora JavaScript SDK.
 * Run with: node test/test-sdk.js
 */

'use strict';

const { NexoraClient, NexoraError } = require('../src/index.js');

// Simple test framework
let passed = 0;
let failed = 0;

function assert(condition, message) {
  if (condition) {
    passed++;
    console.log('  ✓ ' + message);
  } else {
    failed++;
    console.error('  ✗ ' + message);
  }
}

function test(name, fn) {
  console.log('\nTest: ' + name);
  try {
    fn();
  } catch (e) {
    failed++;
    console.error('  ✗ Error: ' + e.message);
  }
}

// Tests
test('NexoraClient constructor', () => {
  const client = new NexoraClient('http://localhost:8080/');
  assert(client.baseUrl === 'http://localhost:8080', 'Should strip trailing slash');
});

test('Hex ID encoding', () => {
  const client = new NexoraClient();
  const hex = client._hexId('test-node');
  assert(hex === '746573742d6e6f6465', 'Should hex-encode node ID');
});

test('Hex ID empty string', () => {
  const client = new NexoraClient();
  const hex = client._hexId('');
  assert(hex === '', 'Empty string should produce empty hex');
});

test('Default base URL', () => {
  const client = new NexoraClient();
  assert(client.baseUrl === 'http://localhost:8080', 'Should use default URL');
});

test('NexoraError is an Error', () => {
  const err = new NexoraError('test');
  assert(err instanceof Error, 'NexoraError should extend Error');
  assert(err.message === 'test', 'Should preserve message');
});

// Summary
setTimeout(() => {
  console.log('\n' + '='.repeat(40));
  console.log(`Results: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}, 100);
