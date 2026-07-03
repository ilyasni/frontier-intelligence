// Общие хелперы для экрана настроек. Числа/статусы/тон — единообразно во всех вкладках.
import { fmtDate } from '../../format.js';

// Компактное число в ru-локали (как в остальной админке).
export function fmtCompact(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '—';
  return n.toLocaleString('ru-RU');
}

// Процент 0..1 → «NN%».
export function fmtPct01(value, digits = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '—';
  return `${(n * 100).toFixed(digits)}%`;
}

// Метка времени: принимает ISO-строку, мс или unix-секунды.
export function fmtTs(value) {
  if (value == null || value === '') return '—';
  if (typeof value === 'number') {
    // unix-секунды (10 знаков) vs миллисекунды
    const ms = value < 1e12 ? value * 1000 : value;
    return fmtDate(ms);
  }
  return fmtDate(value);
}

// Относительная длительность до/после unix-секунд.
export function relDuration(unixSeconds) {
  const target = Number(unixSeconds || 0);
  if (!Number.isFinite(target) || target <= 0) return '—';
  const deltaMs = target * 1000 - Date.now();
  const absSec = Math.round(Math.abs(deltaMs) / 1000);
  const v = absSec >= 3600 ? `${Math.round(absSec / 3600)} ч`
    : absSec >= 60 ? `${Math.round(absSec / 60)} мин`
      : `${absSec} с`;
  return deltaMs >= 0 ? `через ${v}` : `${v} назад`;
}

// Сокращение длинного значения по середине (для URL/DSN).
export function compactMiddle(value, maxLength = 44) {
  const text = String(value == null ? '—' : value);
  if (text.length <= maxLength) return text;
  const left = Math.max(8, Math.floor((maxLength - 1) * 0.6));
  const right = Math.max(8, maxLength - left - 1);
  return `${text.slice(0, left)}…${text.slice(-right)}`;
}

// Тон бейджа по статусу (ok/warn/error → success/warn/danger).
export function toneForStatus(status) {
  const v = String(status || '').trim().toLowerCase();
  if (['ok', 'ready', 'healthy', 'clear', 'tracked', 'live', 'stable', 'active', 'match', 'set', 'present'].includes(v)) return 'success';
  if (['error', 'quota_exhausted', 'drift', 'rate_limited', 'unavailable', 'down', 'fail', 'failed', 'missing', 'mismatch', 'quarantined'].includes(v)) return 'danger';
  if (['warn', 'degraded', 'pending', 'near_cap', 'stale', 'empty', 'unknown', 'low_balance', 'cached', 'shadow-eval', 'maintenance'].includes(v)) return 'warn';
  return 'neutral';
}

// Уникальные непустые строки.
export function uniqueStrings(values) {
  return Array.from(new Set((values || []).map((v) => String(v || '').trim()).filter(Boolean)));
}

// Критичные секреты (остальные — опциональные).
const CRITICAL_SECRETS = [
  'database_url_set', 'gigachat_credentials_set', 'telegram_bot_token_set',
  'telegram_alert_chat_id_set', 'wormsoft_api_key_set', 'openrouter_api_key_set',
];
export function secretIsCritical(key) {
  return CRITICAL_SECRETS.includes(String(key));
}

// Семейства task-family для симулятора и редактора политики.
export const POLICY_FAMILIES = [
  { key: 'text_generation', label: 'Текст (text_generation)' },
  { key: 'vision_generation', label: 'Vision (vision_generation)' },
  { key: 'embeddings', label: 'Эмбеддинги (embeddings)' },
];

export const POLICY_MODES = ['strict', 'degraded', 'maintenance', 'shadow-eval'];

export const SIM_TASKS = [
  { id: 'relevance', label: 'Relevance' },
  { id: 'concepts', label: 'Concepts' },
  { id: 'relevance_concepts', label: 'Relevance + Concepts' },
  { id: 'valence', label: 'Valence' },
  { id: 'mcp_synthesis', label: 'MCP synthesis' },
  { id: 'vision', label: 'Vision analysis' },
  { id: 'embeddings', label: 'Embeddings' },
];

export function familyForTask(task) {
  const t = String(task || '').trim().toLowerCase();
  if (t === 'vision') return 'vision_generation';
  if (['embed', 'embedding', 'embeddings'].includes(t)) return 'embeddings';
  return 'text_generation';
}
