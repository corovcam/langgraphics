import {useEffect, useRef, useState} from "react";
import type {ExecutionEvent, GraphMessage, NodeEntry, WsMessage} from "../types";

const RECONNECT_INTERVAL = 500;
const CONNECTION_TIMEOUT = 500;

export function useWebSocket(url: string) {
    const [events, setEvents] = useState<ExecutionEvent[]>([]);
    const [nodeEntries, setNodeEntries] = useState<NodeEntry[]>([]);
    const [topology, setTopology] = useState<GraphMessage | null>(null);
    const [discoveryEvents, setDiscoveryEvents] = useState<ExecutionEvent[]>([]);
    const [rawMessages, setRawMessages] = useState<WsMessage[]>([]);
    const wsRef = useRef<WebSocket | null>(null);
    const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

    useEffect(() => {
        let unmounted = false;
        let runDone = false;

        function connect() {
            if (unmounted) return;
            const ws = new WebSocket(url);
            wsRef.current = ws;

            timerRef.current = setTimeout(() => {
                if (ws.readyState !== WebSocket.OPEN) {
                    ws.onopen = null;
                    ws.onmessage = null;
                    ws.onclose = null;
                    ws.onerror = null;
                    timerRef.current = setTimeout(connect, RECONNECT_INTERVAL);
                }
            }, CONNECTION_TIMEOUT);

            ws.onopen = () => {
                clearTimeout(timerRef.current!);
                timerRef.current = null;
            };

            ws.onmessage = (event) => {
                if (unmounted) return;
                try {
                    const msg: WsMessage = JSON.parse(event.data);
                    console.log("[ws]", msg.type, msg);
                    setRawMessages((prev) => [...prev.slice(-200), msg]);
                    if (msg.type === "graph") {
                        runDone = false;
                        setEvents([]);
                        setTopology(msg);
                        setNodeEntries([]);
                        setDiscoveryEvents([]);
                    } else if (msg.type === "run_start") {
                        runDone = false;
                        setNodeEntries([]);
                        setEvents((prev) => {
                            // Preserve topology-discovery events so subgraph children
                            // survive the run_start state reset on replay.
                            const topoEvents = prev.filter(
                                (e) => e.type === "node_discovered" || e.type === "edge_discovered",
                            );
                            return [...topoEvents, msg];
                        });
                    } else if (msg.type === "node_output") {
                        const {type: _, ...entry} = msg;
                        setNodeEntries((prev) => {
                            const idx = prev.findIndex((e) => e.run_id === entry.run_id);
                            if (idx >= 0) {
                                const updated = [...prev];
                                updated[idx] = entry;
                                return updated;
                            }
                            return [...prev, entry];
                        });
                        // Also flow into events so computeStatuses can mark the node completed.
                        setEvents((prev) => [...prev, msg as ExecutionEvent]);
                    } else {
                        if (msg.type === "run_end" || msg.type === "error") runDone = true;
                        // node_discovered / edge_discovered also go to discoveryEvents
                        // so buildDynamicTopology only reruns on structural changes.
                        if (msg.type === "node_discovered" || msg.type === "edge_discovered") {
                            setDiscoveryEvents((prev) => [...prev, msg as ExecutionEvent]);
                        }
                        setEvents((prev) => [...prev, msg as ExecutionEvent]);
                    }
                } catch {
                }
            };

            ws.onclose = () => {
                if (!unmounted && !runDone) timerRef.current = setTimeout(connect, RECONNECT_INTERVAL);
            };
            ws.onerror = () => ws.close();
        }

        connect();

        return () => {
            unmounted = true;
            if (timerRef.current) clearTimeout(timerRef.current);
            wsRef.current?.close();
        };
    }, [url]);

    return {topology, events, nodeEntries, discoveryEvents, rawMessages};
}
