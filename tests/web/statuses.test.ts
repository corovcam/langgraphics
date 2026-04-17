import {describe, expect, it} from "vitest";
import {computeStatuses} from "../../langgraphics-web/src/hooks/useGraphState";
import type {ExecutionEvent} from "../../langgraphics-web/src/types";

describe("computeStatuses", () => {
    it("returns empty maps for run_start only", () => {
        const {nodeStatuses, edgeStatuses} = computeStatuses([
            {type: "run_start", run_id: "abc"},
        ]);
        expect(nodeStatuses.size).toBe(0);
        expect(edgeStatuses.size).toBe(0);
    });

    it("sets target node and edge to active on edge_active", () => {
        const {nodeStatuses, edgeStatuses} = computeStatuses([
            {type: "run_start", run_id: "abc"},
            {type: "edge_active", source: "__start__", target: "step_a", edge_id: "e0"},
        ]);
        expect(nodeStatuses.get("step_a")).toBe("active");
        expect(edgeStatuses.get("e0")).toBe("active");
    });

    it("demotes previous active to completed/traversed on next edge_active", () => {
        const {nodeStatuses, edgeStatuses} = computeStatuses([
            {type: "run_start", run_id: "abc"},
            {type: "edge_active", source: "__start__", target: "step_a", edge_id: "e0"},
            {type: "edge_active", source: "step_a", target: "step_b", edge_id: "e1"},
        ]);
        expect(nodeStatuses.get("step_a")).toBe("completed");
        expect(nodeStatuses.get("step_b")).toBe("active");
        expect(edgeStatuses.get("e0")).toBe("traversed");
        expect(edgeStatuses.get("e1")).toBe("active");
    });

    it("finalizes all active to completed/traversed on run_end", () => {
        const {nodeStatuses, edgeStatuses} = computeStatuses([
            {type: "run_start", run_id: "abc"},
            {type: "edge_active", source: "__start__", target: "step_a", edge_id: "e0"},
            {type: "edge_active", source: "step_a", target: "step_b", edge_id: "e1"},
            {type: "edge_active", source: "step_b", target: "__end__", edge_id: "e2"},
            {type: "run_end", run_id: "abc"},
        ]);
        expect(nodeStatuses.get("step_a")).toBe("completed");
        expect(nodeStatuses.get("step_b")).toBe("completed");
        expect(nodeStatuses.get("__end__")).toBe("completed");
        expect(edgeStatuses.get("e0")).toBe("traversed");
        expect(edgeStatuses.get("e1")).toBe("traversed");
        expect(edgeStatuses.get("e2")).toBe("traversed");
    });

    it("sets error status on error event", () => {
        const {nodeStatuses, edgeStatuses} = computeStatuses([
            {type: "run_start", run_id: "abc"},
            {type: "edge_active", source: "__start__", target: "step_a", edge_id: "e0"},
            {type: "error", source: "step_a", target: "step_b", edge_id: "e1"},
        ]);
        expect(nodeStatuses.get("step_a")).toBe("completed");
        expect(nodeStatuses.get("step_b")).toBe("error");
        expect(edgeStatuses.get("e0")).toBe("traversed");
        expect(edgeStatuses.get("e1")).toBe("error");
    });

    it("handles error with null edge_id", () => {
        const {nodeStatuses, edgeStatuses} = computeStatuses([
            {type: "run_start", run_id: "abc"},
            {type: "edge_active", source: "__start__", target: "step_a", edge_id: "e0"},
            {type: "error", source: "step_a", target: "step_a", edge_id: null},
        ]);
        expect(nodeStatuses.get("step_a")).toBe("error");
        expect(edgeStatuses.get("e0")).toBe("traversed");
        expect(edgeStatuses.size).toBe(1);
    });

    it("handles looping graph with repeated node visits", () => {
        const {nodeStatuses, edgeStatuses} = computeStatuses([
            {type: "run_start", run_id: "abc"},
            {type: "edge_active", source: "__start__", target: "process", edge_id: "e0"},
            {type: "edge_active", source: "process", target: "process", edge_id: "e1"},
            {type: "edge_active", source: "process", target: "process", edge_id: "e1"},
            {type: "edge_active", source: "process", target: "__end__", edge_id: "e2"},
            {type: "run_end", run_id: "abc"},
        ]);
        expect(nodeStatuses.get("process")).toBe("completed");
        expect(nodeStatuses.get("__end__")).toBe("completed");
        expect(edgeStatuses.get("e0")).toBe("traversed");
        expect(edgeStatuses.get("e1")).toBe("traversed");
        expect(edgeStatuses.get("e2")).toBe("traversed");
    });

    it("tracks status progression step by step", () => {
        const events: ExecutionEvent[] = [
            {type: "run_start", run_id: "abc"},
        ];

        let result = computeStatuses(events);
        expect(result.nodeStatuses.size).toBe(0);

        events.push({type: "edge_active", source: "__start__", target: "A", edge_id: "e0"});
        result = computeStatuses(events);
        expect(result.nodeStatuses.get("A")).toBe("active");
        expect(result.edgeStatuses.get("e0")).toBe("active");

        events.push({type: "edge_active", source: "A", target: "B", edge_id: "e1"});
        result = computeStatuses(events);
        expect(result.nodeStatuses.get("A")).toBe("completed");
        expect(result.nodeStatuses.get("B")).toBe("active");
        expect(result.edgeStatuses.get("e0")).toBe("traversed");
        expect(result.edgeStatuses.get("e1")).toBe("active");

        events.push({type: "run_end", run_id: "abc"});
        result = computeStatuses(events);
        expect(result.nodeStatuses.get("A")).toBe("completed");
        expect(result.nodeStatuses.get("B")).toBe("completed");
        expect(result.edgeStatuses.get("e0")).toBe("traversed");
        expect(result.edgeStatuses.get("e1")).toBe("traversed");
    });

    it("node_output demotes an active node to completed", () => {
        const {nodeStatuses} = computeStatuses([
            {type: "run_start", run_id: "abc"},
            {type: "edge_active", source: "__start__", target: "step_a", edge_id: "e0"},
            {
                type: "node_output", node_id: "step_a", run_id: "abc",
                node_kind: "chain", status: "ok", input: "{}", output: "{}", state: "{}",
                metrics: {latency: "0ms", costs: {cached: "0", total: "0"}, tokens: {cached: 0, total: 0}},
            },
        ]);
        expect(nodeStatuses.get("step_a")).toBe("completed");
    });

    it("inner subgraph node is completed by node_output, not left active after parent moves on", () => {
        const {nodeStatuses} = computeStatuses([
            {type: "run_start", run_id: "abc"},
            {type: "edge_active", source: "__start__", target: "parent", edge_id: "e0"},
            {type: "node_discovered", node_id: "inner", run_id: "abc", node_kind: "chain", parent_node_id: "parent"},
            {
                type: "node_output", node_id: "inner", run_id: "abc",
                node_kind: "chain", status: "ok", input: "{}", output: "{}", state: "{}",
                metrics: {latency: "0ms", costs: {cached: "0", total: "0"}, tokens: {cached: 0, total: 0}},
            },
            // Parent finishes and next top-level node starts.
            {type: "edge_active", source: "parent", target: "next_node", edge_id: "e1"},
        ]);
        expect(nodeStatuses.get("inner")).toBe("completed");
        expect(nodeStatuses.get("parent")).toBe("completed");
        expect(nodeStatuses.get("next_node")).toBe("active");
    });

    it("run_start clears previous run statuses", () => {
        const {nodeStatuses, edgeStatuses} = computeStatuses([
            {type: "run_start", run_id: "run1"},
            {type: "edge_active", source: "__start__", target: "A", edge_id: "e0"},
            {type: "run_end", run_id: "run1"},
            {type: "run_start", run_id: "run2"},
        ]);
        expect(nodeStatuses.size).toBe(0);
        expect(edgeStatuses.size).toBe(0);
    });
});

import {describe as d2, expect as e2, it as i2} from "vitest";
import {buildDynamicTopology} from "../../langgraphics-web/src/hooks/useGraphState";
import type {ExecutionEvent as EE, GraphMessage} from "../../langgraphics-web/src/types";

d2("buildDynamicTopology", () => {
    const base: GraphMessage = {
        type: "graph",
        nodes: [
            {id: "__start__", name: "__start__", node_type: "start"},
            {id: "outer", name: "outer", node_type: "node"},
            {id: "__end__", name: "__end__", node_type: "end"},
        ],
        edges: [
            {id: "e0", source: "__start__", target: "outer", conditional: false, label: null},
            {id: "e1", source: "outer", target: "__end__", conditional: false, label: null},
        ],
    };

    i2("returns base when no discovery events", () => {
        const result = buildDynamicTopology(base, [{type: "run_start", run_id: "r1"}]);
        e2(result).toBe(base);
    });

    i2("adds subgraph node with parent_id from node_discovered", () => {
        const discoveryEvents: EE[] = [
            {type: "node_discovered", node_id: "inner_step", run_id: "r1", node_kind: "chain", parent_node_id: "outer"},
        ];
        const result = buildDynamicTopology(base, discoveryEvents)!;
        const inner = result.nodes.find((n) => n.id === "inner_step");
        e2(inner).toBeDefined();
        e2(inner?.parent_id).toBe("outer");
        // base nodes preserved
        e2(result.nodes.find((n) => n.id === "outer")).toBeDefined();
    });

    i2("does not overwrite existing node (node_discovered is idempotent)", () => {
        const discoveryEvents: EE[] = [
            {type: "node_discovered", node_id: "outer", run_id: "r1", node_kind: "chain", parent_node_id: null},
        ];
        const result = buildDynamicTopology(base, discoveryEvents)!;
        // outer was in base — should still be there but not duplicated
        e2(result.nodes.filter((n) => n.id === "outer")).toHaveLength(1);
    });

    i2("adds edge from edge_discovered", () => {
        const discoveryEvents: EE[] = [
            {type: "node_discovered", node_id: "inner_step", run_id: "r1", node_kind: "chain", parent_node_id: "outer"},
            {type: "edge_discovered", edge_id: "de0", source: "inner_a", target: "inner_step"},
        ];
        const result = buildDynamicTopology(base, discoveryEvents)!;
        const edge = result.edges.find((e) => e.id === "de0");
        e2(edge?.source).toBe("inner_a");
        e2(edge?.target).toBe("inner_step");
    });

    i2("returns null when base is null regardless of discovery events", () => {
        const discoveryEvents: EE[] = [
            {type: "node_discovered", node_id: "orphan", run_id: "r1", node_kind: "chain", parent_node_id: "missing_parent"},
        ];
        const result = buildDynamicTopology(null, discoveryEvents);
        e2(result).toBeNull();
    });
});
