/**
 * Shesha — ETL pipeline helpers.
 *
 * Small, dependency-free utilities that back the demo dashboard:
 * lineage graph traversal, per-stage throughput / lag rollups, and a
 * "what failed and why" summariser for the last run.
 */

/**
 * Build adjacency maps from a list of edges { from, to }.
 */
export function indexGraph(stages, edges) {
  const successors = new Map();
  const predecessors = new Map();
  for (const s of stages) {
    successors.set(s.id, []);
    predecessors.set(s.id, []);
  }
  for (const { from, to } of edges) {
    if (successors.has(from)) successors.get(from).push(to);
    if (predecessors.has(to)) predecessors.get(to).push(from);
  }
  return { successors, predecessors };
}

/**
 * Downstream impact of a failing stage — the set of stages that won't
 * run because their upstream died.
 */
export function downstreamOf(stageId, successors) {
  const out = new Set();
  const stack = [stageId];
  while (stack.length > 0) {
    const cur = stack.pop();
    for (const next of successors.get(cur) || []) {
      if (!out.has(next)) {
        out.add(next);
        stack.push(next);
      }
    }
  }
  return Array.from(out);
}

/**
 * Summarise stage metrics from a run log.
 *   run.stages: [{ id, recordsIn, recordsOut, failures, durationMs }]
 */
export function summarizeRun(run) {
  const stages = run.stages || [];
  const totalIn = stages.reduce((s, x) => s + (x.recordsIn || 0), 0);
  const totalOut = stages.reduce((s, x) => s + (x.recordsOut || 0), 0);
  const failedStages = stages.filter((x) => (x.failures || 0) > 0);
  const slowest = [...stages].sort((a, b) => (b.durationMs || 0) - (a.durationMs || 0))[0];
  return {
    totalIn,
    totalOut,
    passThrough: totalIn > 0 ? totalOut / totalIn : 0,
    failedStages: failedStages.map((x) => x.id),
    failureCount: failedStages.reduce((s, x) => s + (x.failures || 0), 0),
    slowestStage: slowest?.id,
    slowestStageMs: slowest?.durationMs || 0,
    totalDurationMs: stages.reduce((s, x) => s + (x.durationMs || 0), 0),
  };
}

/**
 * Heuristic: name the *root-cause* stage in a failed run — the
 * earliest failing stage in topological order, since everything
 * downstream of it was doomed anyway.
 */
export function rootCause(run, successors) {
  const failing = new Set((run.stages || []).filter((x) => (x.failures || 0) > 0).map((x) => x.id));
  if (failing.size === 0) return null;
  // A stage is the root cause if none of its predecessors also failed.
  for (const id of failing) {
    const upstream = [];
    for (const [pred, succs] of successors) {
      if (succs.includes(id)) upstream.push(pred);
    }
    if (!upstream.some((p) => failing.has(p))) return id;
  }
  return [...failing][0];
}
