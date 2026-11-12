import type { ActivationDoc, ActivationSummary, CreateActivationBody } from './types';

export const API_URL = (import.meta.env.VITE_API_URL as string | undefined) ?? 'http://localhost:3000';

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${API_URL}${path}`, {
      headers: { 'Content-Type': 'application/json' },
      ...init,
    });
  } catch {
    throw new Error(`No se pudo contactar a la API (${API_URL})`);
  }

  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as { message?: string | string[] };
      if (body?.message) {
        detail = Array.isArray(body.message) ? body.message.join(', ') : body.message;
      }
    } catch {
      // sin cuerpo JSON: dejamos el status
    }
    throw new Error(detail);
  }

  return (await response.json()) as T;
}

/** GET /activations -> resúmenes más recientes. */
export function fetchActivations(): Promise<ActivationSummary[]> {
  return fetchJson<ActivationSummary[]>('/activations');
}

/** GET /activations/:id -> documento completo con historial. */
export function fetchActivation(id: string): Promise<ActivationDoc> {
  return fetchJson<ActivationDoc>(`/activations/${encodeURIComponent(id)}`);
}

/** POST /activations -> 202 { activationId, status } */
export function createActivation(
  body: CreateActivationBody,
): Promise<{ activationId: string; status: string }> {
  return fetchJson<{ activationId: string; status: string }>('/activations', {
    method: 'POST',
    body: JSON.stringify(body),
  });
}
