import {Component, useState} from "react";
import type {ReactNode} from "react";
import {createRoot} from "react-dom/client";
import {type ColorMode} from "@xyflow/react";
import {ReactFlowProvider} from "@xyflow/react";
import {useWebSocket} from "./hooks/useWebSocket";
import {useGraphState} from "./hooks/useGraphState";
import {GraphCanvas} from "./components/GraphCanvas";
import type {ViewMode, InspectorMode, WsMessage} from "./types.ts";
import type {RankDir} from "./layout";
import "@xyflow/react/dist/style.css";
import "./index.css";

const WS_URL = "ws://localhost:8765";

function parseParams(): {theme: ColorMode; direction: RankDir, mode: ViewMode, inspect: InspectorMode, debug: boolean} {
    const p = new URLSearchParams(window.location.search);
    const mode = p.get("mode") ?? "auto";
    const theme = p.get("theme") ?? "system";
    const inspect = p.get("inspect") ?? "off";
    const direction = p.get("direction") ?? "TB";
    return {
        debug: p.has("debug"),
        inspect: (["off", "tree", "full"].includes(inspect) ? inspect : "off") as InspectorMode,
        theme: (["system", "light", "dark"].includes(theme) ? theme : "system") as ColorMode,
        direction: (["TB", "LR"].includes(direction) ? direction : "TB") as RankDir,
        mode: (["auto", "manual"].includes(mode) ? mode : "auto") as ViewMode,
    };
}

/** Scrollable list of raw WebSocket messages, newest first. Enabled via ?debug. */
function DebugPanel({messages}: {messages: WsMessage[]}) {
    const reversed = [...messages].reverse();
    const typeColor: Record<string, string> = {
        graph: "#a78bfa", run_start: "#34d399", run_end: "#34d399",
        edge_active: "#60a5fa", node_discovered: "#f59e0b",
        edge_discovered: "#fb923c", node_output: "#94a3b8",
        error: "#f87171",
    };
    return (
        <div style={{
            position: "fixed", bottom: 0, left: 0, width: "380px", maxHeight: "220px",
            overflowY: "auto", background: "rgba(10,10,10,0.92)", color: "#e2e8f0",
            fontSize: "10px", fontFamily: "monospace", padding: "6px 8px",
            zIndex: 9999, borderTop: "1px solid #334155", borderRight: "1px solid #334155",
        }}>
            <div style={{color: "#64748b", marginBottom: 4, fontWeight: 700}}>
                ▼ DEBUG — {messages.length} messages (newest first)
            </div>
            {reversed.map((msg, i) => (
                <div key={i} style={{marginBottom: 2, color: typeColor[msg.type] ?? "#e2e8f0", wordBreak: "break-all"}}>
                    {JSON.stringify(msg)}
                </div>
            ))}
        </div>
    );
}

class ErrorBoundary extends Component<{children: ReactNode}, {error: string | null}> {
    state = {error: null};
    static getDerivedStateFromError(e: Error) { return {error: e.message}; }
    componentDidCatch(e: Error) { console.error("[ErrorBoundary]", e); }
    render() {
        if (this.state.error) {
            return (
                <div style={{padding: 24, fontFamily: "monospace", color: "#ef4444"}}>
                    <strong>Render error:</strong> {this.state.error}
                    <br/><button onClick={() => this.setState({error: null})}>Retry</button>
                </div>
            );
        }
        return this.props.children;
    }
}

const {theme, mode, inspect, direction, debug} = parseParams();

function Index() {
    const [rankDir, setRankDir] = useState<RankDir>(direction);
    const {topology, events, nodeEntries, discoveryEvents, rawMessages} = useWebSocket(WS_URL);
    const {nodes, edges, activeNodeIds} = useGraphState(topology, events, discoveryEvents, rankDir);

    return (
        <ReactFlowProvider>
            <GraphCanvas
                nodes={nodes}
                edges={edges}
                events={events}
                initialMode={mode}
                initialInspect={inspect}
                initialColorMode={theme}
                nodeEntries={nodeEntries}
                initialRankDir={direction}
                onRankDirChange={setRankDir}
                activeNodeIds={activeNodeIds}
            />
            {debug && <DebugPanel messages={rawMessages}/>}
        </ReactFlowProvider>
    );
}

createRoot(document.getElementById("root")!).render(
    <ErrorBoundary><Index/></ErrorBoundary>,
);
