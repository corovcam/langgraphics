import dagre from "@dagrejs/dagre";
import {Position} from "@xyflow/react";
import type {Edge, Node} from "@xyflow/react";
import type {EdgeData, GraphMessage, NodeData, ProtocolEdge, ProtocolNode} from "./types";

export type RankDir = "TB" | "LR";

const NODE_WIDTH = 180;
const NODE_HEIGHT = 60;
const SMALL_NODE_WIDTH = 120;
const SMALL_NODE_HEIGHT = 40;

/** Dimensions for child nodes rendered inside a parent container. */
const CHILD_NODE_WIDTH = 150;
const CHILD_NODE_HEIGHT = 40;
/** Space above the first child row (reserved for the parent's label). */
const CHILD_LABEL_H = 34;
/** Horizontal padding on each side of the child area inside the parent. */
const CHILD_PAD_X = 12;
/** Vertical padding between the child area and the parent's bottom border. */
const CHILD_PAD_Y = 10;

const DIRECTIONS_MAP: Record<string, Position> = {
    T: Position.Top, L: Position.Left,
    R: Position.Right, B: Position.Bottom,
};

/**
 * Lays out a group of child nodes using a mini-dagre pass.
 * Returns the bounding-box size and per-node positions relative to the group centre.
 */
function computeChildSubLayout(
    children: ProtocolNode[],
    childEdges: ProtocolEdge[],
    rankDir: RankDir,
): {width: number; height: number; positions: Map<string, {x: number; y: number}>} {
    if (children.length === 0) return {width: 0, height: 0, positions: new Map()};

    // Fast path for a single child — skip dagre entirely.
    if (children.length === 1) {
        return {
            width: CHILD_NODE_WIDTH,
            height: CHILD_NODE_HEIGHT,
            positions: new Map([[children[0].id, {x: 0, y: 0}]]),
        };
    }

    const g = new dagre.graphlib.Graph();
    g.setDefaultEdgeLabel(() => ({}));
    g.setGraph({rankdir: rankDir, ranksep: 40, nodesep: 30, marginx: 0, marginy: 0});
    for (const c of children) g.setNode(c.id, {width: CHILD_NODE_WIDTH, height: CHILD_NODE_HEIGHT});
    for (const e of childEdges) {
        if (g.hasNode(e.source) && g.hasNode(e.target)) g.setEdge(e.source, e.target);
    }
    dagre.layout(g);

    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    const rawPositions = new Map<string, {x: number; y: number}>();
    for (const c of children) {
        const pos = g.node(c.id);
        const x = isFinite(pos?.x) ? pos.x : 0;
        const y = isFinite(pos?.y) ? pos.y : 0;
        rawPositions.set(c.id, {x, y});
        minX = Math.min(minX, x - CHILD_NODE_WIDTH / 2);
        minY = Math.min(minY, y - CHILD_NODE_HEIGHT / 2);
        maxX = Math.max(maxX, x + CHILD_NODE_WIDTH / 2);
        maxY = Math.max(maxY, y + CHILD_NODE_HEIGHT / 2);
    }

    const cx = (minX + maxX) / 2;
    const cy = (minY + maxY) / 2;
    const positions = new Map([...rawPositions].map(([id, {x, y}]) => [id, {x: x - cx, y: y - cy}]));
    return {width: maxX - minX, height: maxY - minY, positions};
}

/** Expanded parent dimensions when it contains child nodes. */
function parentContainerDims(sub: {width: number; height: number}): {width: number; height: number} {
    return {
        width: Math.max(NODE_WIDTH, sub.width + 2 * CHILD_PAD_X),
        height: CHILD_LABEL_H + sub.height + CHILD_PAD_Y,
    };
}

export function computeLayout(topology: GraphMessage, rankDir: RankDir = "TB"): {
    nodes: Node<NodeData>[];
    edges: Edge<EdgeData>[];
} {
    const RANK_TO = DIRECTIONS_MAP[rankDir[1]] as Position;
    const RANK_FROM = DIRECTIONS_MAP[rankDir[0]] as Position;
    const IS_HORIZONTAL = ["LR", "RL"].includes(rankDir);

    // Separate nodes and edges by level.
    const childrenOf = new Map<string, ProtocolNode[]>();
    const topLevelNodes: ProtocolNode[] = [];
    for (const n of topology.nodes) {
        if (n.parent_id) {
            if (!childrenOf.has(n.parent_id)) childrenOf.set(n.parent_id, []);
            childrenOf.get(n.parent_id)!.push(n);
        } else {
            topLevelNodes.push(n);
        }
    }
    const childNodeIds = new Set(topology.nodes.filter((n) => n.parent_id).map((n) => n.id));
    const topLevelEdges = topology.edges.filter(
        (e) => !childNodeIds.has(e.source) && !childNodeIds.has(e.target),
    );
    const childEdges = topology.edges.filter(
        (e) => childNodeIds.has(e.source) || childNodeIds.has(e.target),
    );

    // Compute sub-layouts for each group of children.
    const childSubLayouts = new Map<string, ReturnType<typeof computeChildSubLayout>>();
    for (const [parentId, children] of childrenOf) {
        const edges = childEdges.filter(
            (e) => children.some((c) => c.id === e.source || c.id === e.target),
        );
        childSubLayouts.set(parentId, computeChildSubLayout(children, edges, rankDir));
    }

    // Main dagre: use full container dimensions for parent nodes.
    const g = new dagre.graphlib.Graph();
    g.setDefaultEdgeLabel(() => ({}));
    g.setGraph({rankdir: rankDir, ranksep: 80, nodesep: 60, marginx: 20, marginy: 20});

    for (const n of topLevelNodes) {
        const isTerminal = n.node_type === "start" || n.node_type === "end";
        const sub = childSubLayouts.get(n.id);
        if (sub) {
            const {width, height} = parentContainerDims(sub);
            g.setNode(n.id, {width, height});
        } else {
            g.setNode(n.id, {
                width: isTerminal ? SMALL_NODE_WIDTH : NODE_WIDTH,
                height: isTerminal ? SMALL_NODE_HEIGHT : NODE_HEIGHT,
            });
        }
    }
    for (const e of topLevelEdges) g.setEdge(e.source, e.target);
    dagre.layout(g);

    const nodeX = new Map<string, number>(topLevelNodes.map((n) => [n.id, g.node(n.id).x]));
    const nodeY = new Map<string, number>(topLevelNodes.map((n) => [n.id, g.node(n.id).y]));

    const nodeRank = IS_HORIZONTAL ? nodeX : nodeY;
    const nodeCross = IS_HORIZONTAL ? nodeY : nodeX;
    const crossSize = IS_HORIZONTAL ? NODE_HEIGHT : NODE_WIDTH;

    const isBack = (e: {source: string; target: string}) =>
        (nodeRank.get(e.source) ?? 0) >= (nodeRank.get(e.target) ?? 0);

    for (const be of topLevelEdges.filter(isBack)) {
        const tRank = nodeRank.get(be.target) ?? 0;
        const minRank = Math.min(nodeRank.get(be.source) ?? 0, tRank);
        const maxRank = Math.max(nodeRank.get(be.source) ?? 0, tRank);
        const corridor = ((nodeCross.get(be.source) ?? 0) + (nodeCross.get(be.target) ?? 0)) / 2;
        for (const n of topLevelNodes) {
            if (n.id === be.source || n.id === be.target) continue;
            const nRank = nodeRank.get(n.id) ?? 0;
            const nCross = nodeCross.get(n.id) ?? 0;
            if (nRank > minRank && nRank < maxRank && Math.abs(nCross - corridor) < crossSize / 2) {
                nodeCross.set(n.id, nCross - crossSize * 0.5);
            }
        }
    }

    const edgeNeighborCross = new Map<string, number>();
    const buckets = new Map<string, Map<Position, string[]>>();
    const bucket = (nodeId: string, pos: Position) => {
        if (!buckets.has(nodeId)) buckets.set(nodeId, new Map());
        const m = buckets.get(nodeId)!;
        if (!m.has(pos)) m.set(pos, []);
        return m.get(pos)!;
    };
    for (const e of topLevelEdges) {
        const back = isBack(e);
        bucket(e.source, back ? RANK_FROM : RANK_TO).push(`src:${e.id}`);
        bucket(e.target, back ? RANK_TO : RANK_FROM).push(`tgt:${e.id}`);
        edgeNeighborCross.set(`src:${e.id}`, nodeCross.get(e.target) ?? 0);
        edgeNeighborCross.set(`tgt:${e.id}`, nodeCross.get(e.source) ?? 0);
    }
    for (const positions of buckets.values()) {
        for (const ids of positions.values()) {
            ids.sort((a, b) => (edgeNeighborCross.get(a) ?? 0) - (edgeNeighborCross.get(b) ?? 0));
        }
    }

    const nodes: Node<NodeData>[] = [];

    for (const n of topLevelNodes) {
        const isTerminal = n.node_type === "start" || n.node_type === "end";
        const sub = childSubLayouts.get(n.id);
        const w = sub
            ? parentContainerDims(sub).width
            : isTerminal ? SMALL_NODE_WIDTH : NODE_WIDTH;
        const h = sub
            ? parentContainerDims(sub).height
            : isTerminal ? SMALL_NODE_HEIGHT : NODE_HEIGHT;
        const px = nodeX.get(n.id) ?? g.node(n.id).x;
        const py = nodeY.get(n.id) ?? g.node(n.id).y;

        const handles: NodeData["handles"] = [];
        for (const [position, ids] of buckets.get(n.id) ?? []) {
            const step = 100 / (ids.length + 1);
            ids.forEach((id, i) =>
                handles.push({
                    id,
                    position,
                    type: id.startsWith("src:") ? "source" : "target",
                    style: IS_HORIZONTAL
                        ? {top: `${step * (i + 1)}%`, transform: "translateY(-50%)"}
                        : {left: `${step * (i + 1)}%`, transform: "translateX(-50%)"},
                }),
            );
        }

        nodes.push({
            id: n.id,
            type: "custom",
            position: {x: px - w / 2, y: py - h / 2},
            // Explicit style required so xyflow knows the container dimensions.
            style: sub ? {width: w, height: h} : undefined,
            data: {
                label: n.name,
                nodeType: n.node_type,
                status: "idle" as const,
                handles,
                isGroup: !!sub,
            },
        });

        // Place children at positions relative to the parent container's top-left.
        if (sub) {
            // Centre of the child area within the parent.
            const childAreaCenterX = w / 2;
            const childAreaCenterY = CHILD_LABEL_H + sub.height / 2 + CHILD_PAD_Y / 2;
            const childTargetHandle: NodeData["handles"][0] = {
                id: "child-target",
                position: RANK_FROM,
                type: "target",
                style: IS_HORIZONTAL ? {top: "50%", transform: "translateY(-50%)"} : {left: "50%", transform: "translateX(-50%)"},
            };
            const childSourceHandle: NodeData["handles"][0] = {
                id: "child-source",
                position: RANK_TO,
                type: "source",
                style: IS_HORIZONTAL ? {top: "50%", transform: "translateY(-50%)"} : {left: "50%", transform: "translateX(-50%)"},
            };

            for (const child of childrenOf.get(n.id) ?? []) {
                const rel = sub.positions.get(child.id) ?? {x: 0, y: 0};
                nodes.push({
                    id: child.id,
                    type: "custom",
                    parentId: n.id,
                    extent: "parent",
                    position: {
                        x: childAreaCenterX + rel.x - CHILD_NODE_WIDTH / 2,
                        y: childAreaCenterY + rel.y - CHILD_NODE_HEIGHT / 2,
                    },
                    style: {width: CHILD_NODE_WIDTH, height: CHILD_NODE_HEIGHT},
                    data: {
                        label: child.name,
                        nodeType: "node" as const,
                        status: "idle" as const,
                        handles: [childTargetHandle, childSourceHandle],
                        parentNodeId: n.id,
                    },
                });
            }
        }
    }

    // Top-level edges: custom handles for precise routing.
    // Child edges: dedicated child-source/child-target handles.
    const edges: Edge<EdgeData>[] = topology.edges.map((e) => {
        const isChildEdge = childNodeIds.has(e.source) || childNodeIds.has(e.target);
        return {
            id: e.id,
            source: e.source,
            target: e.target,
            ...(isChildEdge
                ? {sourceHandle: "child-source", targetHandle: "child-target"}
                : {sourceHandle: `src:${e.id}`, targetHandle: `tgt:${e.id}`}),
            data: {conditional: e.conditional, label: e.label, status: "idle" as const},
        };
    });

    return {nodes, edges};
}

