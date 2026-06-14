/**
 * examples/node-demo.cjs — Node.js CommonJS usage example
 *
 * Run: node examples/node-demo.cjs
 */
'use strict';

const {
  Event,
  JcKernel,
  KvFunctor, LogFunctor, CounterFunctor, LwwFunctor,
  DistributedNode, mergeHistories,
} = require('../jc-computation.standalone.js');

console.log('═══════════════════════════════════════════════════');
console.log('  JC-Computation — Node.js Demo');
console.log('  State = σ(nf(History))');
console.log('═══════════════════════════════════════════════════\n');

// ── Demo 1: Key-Value Store ────────────────────────────────────────────────
console.log('── Demo 1: Key-Value Store ──────────────────────────');
{
  const k = new JcKernel();
  k.append(k.newEvent('set', { key: 'name', val: 'Alice' }));
  k.append(k.newEvent('set', { key: 'role', val: 'admin' }));
  k.append(k.newEvent('set', { key: 'name', val: 'Bob' })); // overwrites

  const state = k.state(KvFunctor);
  console.log(`  History: ${k.historySize} events (state derived — never stored)`);
  for (const [k2, v] of [...state.entries()].sort()) {
    console.log(`    ${k2} = ${JSON.stringify(v)}`);
  }
  console.assert(state.get('name') === 'Bob', 'last write wins');
  console.log('  ✓\n');
}

// ── Demo 2: Distributed Counter ────────────────────────────────────────────
console.log('── Demo 2: Distributed Counter ─────────────────────');
{
  const k = new JcKernel();
  for (const d of [1, 5, 3, -2, 10]) k.append(k.newEvent('increment', d));
  const total = k.state(CounterFunctor);
  console.log(`  Increments: [1, 5, 3, -2, 10]`);
  console.log(`  Total: ${total}`);
  console.assert(total === 17, 'counter sum');
  console.log('  ✓\n');
}

// ── Demo 3: Causal Log with Noop Elimination ───────────────────────────────
console.log('── Demo 3: Causal Log + Noop Elimination ────────────');
{
  const k = new JcKernel();
  k.append(k.newEvent('log', 'system started'));
  k.append(k.newNoop());                          // will be eliminated
  k.append(k.newEvent('log', 'user logged in'));
  k.append(k.newNoop());                          // will be eliminated
  k.append(k.newEvent('log', 'action performed'));

  const log = k.state(LogFunctor);
  log.forEach(e => console.log(`  → ${e}`));
  console.assert(log.length === 3, 'noops invisible in log');
  console.log(`  ✓ ${log.length} entries (${k.totalStats.noopsEliminated} noops eliminated)\n`);
}

// ── Demo 4: Last-Write-Wins Register ──────────────────────────────────────
console.log('── Demo 4: LWW Register ─────────────────────────────');
{
  const k = new JcKernel();
  k.append(k.newEvent('write', { key: 'config', val: { debug: false }, ts: 100 }));
  k.append(k.newEvent('write', { key: 'config', val: { debug: true },  ts: 200 }));
  k.append(k.newEvent('write', { key: 'config', val: { debug: false }, ts: 50  })); // stale

  const state = k.state(LwwFunctor);
  const cfg = state.get('config');
  console.log(`  config = ${JSON.stringify(cfg.val)} (ts=${cfg.ts})`);
  console.assert(cfg.val.debug === true, 'highest ts wins');
  console.log('  ✓\n');
}

// ── Demo 5: Distributed Convergence ───────────────────────────────────────
console.log('── Demo 5: Distributed Convergence ─────────────────');
console.log('  Two nodes partition, advance independently, then sync.');
console.log('  No consensus — convergence IS the normal form.\n');
{
  const nodeA = new DistributedNode('Node-A');
  const nodeB = new DistributedNode('Node-B');

  nodeA.append(nodeA.newEvent('increment', 100));
  nodeA.append(nodeA.newEvent('increment', 50));
  nodeB.append(nodeB.newEvent('increment', 25));
  nodeB.append(nodeB.newEvent('increment', 75));

  console.log(`  Before sync:  A=${nodeA.state(CounterFunctor)}  B=${nodeB.state(CounterFunctor)}`);

  nodeA.syncWith(nodeB);
  nodeB.syncWith(nodeA);

  const ca = nodeA.state(CounterFunctor);
  const cb = nodeB.state(CounterFunctor);
  console.log(`  After sync:   A=${ca}  B=${cb}`);
  console.assert(ca === cb && ca === 250, 'convergence');
  console.log(`  ✓ Converged to ${ca}\n`);
}

// ── Demo 6: Three-way merge ────────────────────────────────────────────────
console.log('── Demo 6: Three-way Merge ──────────────────────────');
{
  const nodeA = new DistributedNode('A');
  const nodeB = new DistributedNode('B');
  const nodeC = new DistributedNode('C');

  nodeA.append(nodeA.newEvent('set', { key: 'x', val: 'from-A' }));
  nodeB.append(nodeB.newEvent('set', { key: 'y', val: 'from-B' }));
  nodeC.append(nodeC.newEvent('set', { key: 'z', val: 'from-C' }));

  // Full gossip round
  for (const [n1, n2] of [[nodeA,nodeB],[nodeA,nodeC],[nodeB,nodeC]]) {
    n1.syncWith(n2); n2.syncWith(n1);
  }

  const states = [nodeA, nodeB, nodeC].map(n => n.state(KvFunctor));
  console.assert(states.every(s => s.get('x') === 'from-A'), 'x visible everywhere');
  console.assert(states.every(s => s.get('y') === 'from-B'), 'y visible everywhere');
  console.assert(states.every(s => s.get('z') === 'from-C'), 'z visible everywhere');
  console.log('  All keys visible on all nodes:');
  for (const [k, v] of [...states[0].entries()].sort()) {
    console.log(`    ${k} = ${JSON.stringify(v)}`);
  }
  console.log('  ✓\n');
}

// ── Demo 7: Custom Semantic Functor ───────────────────────────────────────
console.log('── Demo 7: Custom Semantic Functor ──────────────────');
console.log('  σ = a 2P-Set (two-phase set CRDT)');
{
  const TwoPhaseSetFunctor = {
    interpret(dag) {
      const added   = new Set();
      const removed = new Set();
      for (const e of dag.events.values()) {
        if (!e.isData()) continue;
        if (e.payload.kind === 'add') added.add(e.payload.value);
        if (e.payload.kind === 'rem') removed.add(e.payload.value);
      }
      return new Set([...added].filter(x => !removed.has(x)));
    },
  };

  const k = new JcKernel();
  k.append(k.newEvent('add', 'apple'));
  k.append(k.newEvent('add', 'banana'));
  k.append(k.newEvent('add', 'cherry'));
  k.append(k.newEvent('rem', 'banana'));

  const set = k.state(TwoPhaseSetFunctor);
  console.log(`  Set: {${[...set].sort().join(', ')}}`);
  console.assert(set.has('apple') && !set.has('banana') && set.has('cherry'));
  console.log('  ✓\n');
}

console.log('═══════════════════════════════════════════════════');
console.log('  All demos passed. State never stored — always σ(nf(H)).');
console.log('═══════════════════════════════════════════════════');
