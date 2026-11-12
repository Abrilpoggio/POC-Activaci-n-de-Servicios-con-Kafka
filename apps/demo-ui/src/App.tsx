import { useCallback, useEffect, useRef, useState, type FormEvent } from 'react';
import { createActivation, fetchActivation, fetchActivations } from './api';
import { socket } from './socket';
import type {
  ActivationDoc,
  ActivationStatus,
  ActivationSummary,
  SimulateFailure,
  TimelineEvent,
} from './types';

const PLAN_OPTIONS = ['Plan Básico', 'Plan Digital', 'Plan Premium'];

const FAILURE_OPTIONS: Array<{ label: string; value: SimulateFailure }> = [
  { label: 'Ninguno', value: 'none' },
  { label: 'En billing', value: 'billing' },
  { label: 'En provisioning', value: 'provisioning' },
];

const STATUS_LABEL: Record<ActivationStatus, string> = {
  PENDING: 'Pendiente',
  IN_PROGRESS: 'En curso',
  ACTIVE: 'Activa',
  FAILED: 'Fallida',
};

function formatTime(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return '—';
  return date.toLocaleTimeString('es-AR', { hour12: false });
}

function reasonOf(payload: Record<string, unknown>): string | null {
  const reason = payload['reason'];
  return typeof reason === 'string' && reason.trim() !== '' ? reason : null;
}

/** Badge de estado con color según la saga. */
function StatusBadge({ status }: { status: ActivationStatus }) {
  return <span className={`badge badge-${status.toLowerCase()}`}>{STATUS_LABEL[status]}</span>;
}

/** Item de la lista lateral de activaciones. */
function ActivationListItem({
  item,
  selected,
  onSelect,
}: {
  item: ActivationSummary;
  selected: boolean;
  onSelect: () => void;
}) {
  return (
    <li>
      <button
        type="button"
        className={`activation-item${selected ? ' is-selected' : ''}`}
        onClick={onSelect}
      >
        <span className="activation-item__id">{item.activationId}</span>
        <span className="activation-item__meta">
          {item.customerId} · {item.planId}
        </span>
        <span className="activation-item__footer">
          <StatusBadge status={item.status} />
          <span className="activation-item__events">{item.events} eventos</span>
        </span>
      </button>
    </li>
  );
}

/** Un evento del timeline: punto de color, tipo, hora, source y reason. */
function TimelineRow({ event }: { event: TimelineEvent }) {
  const isCompleted = event.eventType === 'ActivationCompleted';
  const isFailed = event.eventType === 'ActivationFailed';
  const tone = isCompleted ? 'ok' : isFailed ? 'ko' : 'neutral';
  const reason = reasonOf(event.payload ?? {});

  return (
    <li className={`timeline__row timeline__row--${tone}`}>
      <span className="timeline__dot" aria-hidden="true" />
      <div className="timeline__body">
        <div className="timeline__head">
          <span className="timeline__type">{event.eventType}</span>
          <time className="timeline__time" dateTime={event.occurredAt}>
            {formatTime(event.occurredAt)}
          </time>
        </div>
        <div className="timeline__source">source: {event.source}</div>
        {reason ? <div className="timeline__reason">{reason}</div> : null}
      </div>
    </li>
  );
}

/** Paso billing / provisioning, chiquito, junto al título. */
function StepChip({ label, step }: { label: string; step: NonNullable<NonNullable<ActivationDoc['steps']>['billing']> }) {
  return (
    <span className={`step-chip step-chip--${step.status === 'OK' ? 'ok' : 'ko'}`}>
      {label}: {step.status}
    </span>
  );
}

export default function App() {
  const [connected, setConnected] = useState(socket.connected);
  const [activations, setActivations] = useState<ActivationSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [doc, setDoc] = useState<ActivationDoc | null>(null);
  const [docError, setDocError] = useState<string | null>(null);

  const [customerId, setCustomerId] = useState('C-1234');
  const [planId, setPlanId] = useState(PLAN_OPTIONS[0]);
  const [simulateFailure, setSimulateFailure] = useState<SimulateFailure>('none');
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState<string | null>(null);
  const [formError, setFormError] = useState<string | null>(null);

  const selectedRef = useRef<string | null>(null);
  selectedRef.current = selectedId;

  /** Refresca el listado (silencioso: no pisa errores de la UI). */
  const refreshList = useCallback(async () => {
    try {
      setActivations(await fetchActivations());
    } catch {
      // la API puede estar baja: lo muestra el próximo intento visible
    }
  }, []);

  /** Carga el documento completo de una activación. */
  const selectActivation = useCallback(async (id: string) => {
    setSelectedId(id);
    selectedRef.current = id;
    setDoc(null);
    setDocError(null);
    try {
      const fetched = await fetchActivation(id);
      if (selectedRef.current === id) setDoc(fetched);
    } catch (error) {
      if (selectedRef.current === id) {
        setDocError(error instanceof Error ? error.message : 'Error al cargar la activación');
      }
    }
  }, []);

  // WebSocket: estado de conexión + eventos de timeline en tiempo real.
  useEffect(() => {
    const onConnect = () => setConnected(true);
    const onDisconnect = () => setConnected(false);
    const onTimeline = (event: TimelineEvent) => {
      if (!event || typeof event.correlationId !== 'string') return;

      // El listado siempre refleja el cambio de estado.
      void refreshList();

      if (event.correlationId !== selectedRef.current) return;

      setDoc((current) => {
        if (!current) return current;
        const entry = {
          eventType: event.eventType,
          at: event.occurredAt,
          source: event.source,
          payload: event.payload ?? {},
        };
        const duplicated = current.history.some(
          (h) => h.eventType === entry.eventType && h.at === entry.at && h.source === entry.source,
        );
        const history = duplicated ? current.history : [...current.history, entry];
        const status =
          event.status && event.status !== current.status
            ? ((event.status as ActivationStatus) ?? current.status)
            : current.status;
        return { ...current, history, status };
      });
    };

    socket.on('connect', onConnect);
    socket.on('disconnect', onDisconnect);
    socket.on('timeline', onTimeline);
    setConnected(socket.connected);

    return () => {
      socket.off('connect', onConnect);
      socket.off('disconnect', onDisconnect);
      socket.off('timeline', onTimeline);
    };
  }, [refreshList]);

  // Carga inicial + polling liviano del listado.
  useEffect(() => {
    void refreshList();
    const timer = window.setInterval(() => void refreshList(), 5000);
    return () => window.clearInterval(timer);
  }, [refreshList]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setFormError(null);
    setFeedback(null);

    if (!customerId.trim()) {
      setFormError('Ingresá un customerId.');
      return;
    }

    setSubmitting(true);
    try {
      const created = await createActivation({
        customerId: customerId.trim(),
        planId,
        channel: 'web',
        simulateFailure,
      });
      setFeedback(`Activación ${created.activationId} creada (${created.status})`);
      await refreshList();
      await selectActivation(created.activationId);
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'No se pudo crear la activación');
    } finally {
      setSubmitting(false);
    }
  };

  const finalStatus = doc?.status;
  const steps = doc?.steps;

  return (
    <div className="app">
      <header className="header">
        <div className="header__brand">
          <span className="header__logo" aria-hidden="true">⚡</span>
          <div>
            <h1>POC · Activación de Servicios con Kafka</h1>
            <p className="header__subtitle">Saga de activación en tiempo real</p>
          </div>
        </div>
        <div className={`ws-status ${connected ? 'is-on' : 'is-off'}`}>
          <span className="ws-status__dot" aria-hidden="true" />
          {connected ? 'conectado' : 'desconectado'}
        </div>
      </header>

      <main className="layout">
        <aside className="panel panel--left">
          <section className="card">
            <h2 className="card__title">Contratar plan</h2>
            <form className="form" onSubmit={handleSubmit}>
              <label className="field">
                <span className="field__label">customerId</span>
                <input
                  className="input"
                  type="text"
                  value={customerId}
                  onChange={(e) => setCustomerId(e.target.value)}
                  placeholder="C-1234"
                  autoComplete="off"
                />
              </label>

              <label className="field">
                <span className="field__label">Plan</span>
                <select className="input" value={planId} onChange={(e) => setPlanId(e.target.value)}>
                  {PLAN_OPTIONS.map((plan) => (
                    <option key={plan} value={plan}>
                      {plan}
                    </option>
                  ))}
                </select>
              </label>

              <label className="field">
                <span className="field__label">Forzar fallo</span>
                <select
                  className="input"
                  value={simulateFailure}
                  onChange={(e) => setSimulateFailure(e.target.value as SimulateFailure)}
                >
                  {FAILURE_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <button className="button" type="submit" disabled={submitting}>
                {submitting ? 'Contratando…' : 'Contratar plan'}
              </button>

              {feedback ? <p className="feedback feedback--ok">{feedback}</p> : null}
              {formError ? <p className="feedback feedback--ko">{formError}</p> : null}
            </form>
          </section>

          <section className="card card--grow">
            <div className="card__head">
              <h2 className="card__title">Activaciones recientes</h2>
              <button className="link-button" type="button" onClick={() => void refreshList()}>
                Refrescar
              </button>
            </div>
            {activations.length === 0 ? (
              <p className="empty">Todavía no hay activaciones.</p>
            ) : (
              <ul className="activation-list">
                {activations.map((item) => (
                  <ActivationListItem
                    key={item.activationId}
                    item={item}
                    selected={item.activationId === selectedId}
                    onSelect={() => void selectActivation(item.activationId)}
                  />
                ))}
              </ul>
            )}
          </section>
        </aside>

        <section className="panel panel--right">
          {!selectedId ? (
            <div className="card card--empty">
              <p className="empty">Seleccioná una activación para ver su timeline.</p>
            </div>
          ) : docError ? (
            <div className="card">
              <p className="feedback feedback--ko">{docError}</p>
            </div>
          ) : !doc ? (
            <div className="card card--empty">
              <p className="empty">Cargando activación {selectedId}…</p>
            </div>
          ) : (
            <div className="card card--grow">
              <div className="detail__head">
                <div>
                  <div className="detail__title-row">
                    <h2 className="card__title">{doc.activationId}</h2>
                    <StatusBadge status={doc.status} />
                  </div>
                  <p className="detail__meta">
                    {doc.customerId} · {doc.planId}
                    {doc.simulateFailure && doc.simulateFailure !== 'none'
                      ? ` · fallo simulado: ${doc.simulateFailure}`
                      : ''}
                  </p>
                  <div className="steps">
                    {steps?.billing ? <StepChip label="billing" step={steps.billing} /> : null}
                    {steps?.provisioning ? (
                      <StepChip label="provisioning" step={steps.provisioning} />
                    ) : null}
                  </div>
                </div>
              </div>

              {finalStatus === 'ACTIVE' ? (
                <div className="summary summary--ok">✅ Activación completada</div>
              ) : finalStatus === 'FAILED' ? (
                <div className="summary summary--ko">❌ Activación fallida (compensación)</div>
              ) : null}

              <ol className="timeline">
                {doc.history.map((entry, index) => (
                  <TimelineRow
                    key={`${entry.eventType}-${entry.at}-${index}`}
                    event={{
                      activationId: doc.activationId,
                      correlationId: doc.activationId,
                      eventType: entry.eventType,
                      occurredAt: entry.at,
                      source: entry.source,
                      status: doc.status,
                      payload: entry.payload,
                    }}
                  />
                ))}
              </ol>
            </div>
          )}
        </section>
      </main>
    </div>
  );
}
