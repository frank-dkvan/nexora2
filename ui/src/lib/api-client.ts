import { ApiError } from '../types';

const BASE = '';

async function request<T>(
  url: string,
  opts: RequestInit = {},
): Promise<T> {
  // Only set Content-Type for requests with a body (POST/PUT)
  const headers: Record<string, string> = { ...(opts.headers as Record<string, string> || {}) };
  if (opts.body && !headers['Content-Type']) {
    headers['Content-Type'] = 'application/json';
  }

  const res = await fetch(`${BASE}${url}`, {
    ...opts,
    headers,
  });

  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    // 404 for missing properties is normal — don't throw, return empty
    if (res.status === 404) {
      return { value: null, notFound: true } as unknown as T;
    }
    throw new ApiError(res.status, text);
  }

  const contentType = res.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    return res.json() as Promise<T>;
  }
  return res.text() as unknown as T;
}

export const api = {
  get<T>(url: string): Promise<T> {
    return request<T>(url);
  },

  post<T>(url: string, body?: unknown): Promise<T> {
    return request<T>(url, {
      method: 'POST',
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
  },

  put<T>(url: string, body?: unknown): Promise<T> {
    return request<T>(url, {
      method: 'PUT',
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
  },

  del<T>(url: string): Promise<T> {
    return request<T>(url, { method: 'DELETE' });
  },
};
