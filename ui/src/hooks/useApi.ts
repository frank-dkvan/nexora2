import { useState, useEffect, useCallback, useRef } from 'react';
import { api } from '../lib';

export function usePolling<T>(
  url: string,
  intervalMs: number,
  enabled = true,
): { data: T | null; error: string | null; loading: boolean } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    try {
      const result = await api.get<T>(url);
      setData(result);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, [url]);

  useEffect(() => {
    if (!enabled) return;
    fetchData();
    const id = setInterval(fetchData, intervalMs);
    return () => clearInterval(id);
  }, [fetchData, intervalMs, enabled]);

  return { data, error, loading };
}

export function useWebSocket(
  url: string,
  onMessage: (data: unknown) => void,
): { send: (msg: unknown) => void; close: () => void; ready: boolean } {
  const wsRef = useRef<WebSocket | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}${url}`;
    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => setReady(true);
    ws.onclose = () => setReady(false);
    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        onMessage(data);
      } catch {
        onMessage(event.data);
      }
    };

    return () => {
      ws.close();
    };
  }, [url, onMessage]);

  const send = useCallback((msg: unknown) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(typeof msg === 'string' ? msg : JSON.stringify(msg));
    }
  }, []);

  const close = useCallback(() => {
    wsRef.current?.close();
  }, []);

  return { send, close, ready };
}

export function useApi<T>(
  url: string | null,
): { data: T | null; error: string | null; loading: boolean; refetch: () => void } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchFn = useCallback(async () => {
    if (!url) return;
    setLoading(true);
    try {
      const result = await api.get<T>(url);
      setData(result);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, [url]);

  useEffect(() => {
    fetchFn();
  }, [fetchFn]);

  return { data, error, loading, refetch: fetchFn };
}
