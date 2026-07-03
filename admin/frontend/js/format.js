// Форматтеры отображения. Vue-шаблоны экранируют вывод сами — отдельного escape не нужно.

export function fmtDate(v, withTime = true) {
  if (!v) return '—';
  const d = new Date(v);
  if (isNaN(d.getTime())) return String(v);
  const opts = withTime
    ? { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }
    : { year: 'numeric', month: '2-digit', day: '2-digit' };
  return d.toLocaleString('ru-RU', opts);
}

export function fmtAgo(v) {
  if (!v) return '—';
  const d = new Date(v);
  if (isNaN(d.getTime())) return String(v);
  const sec = Math.floor((Date.now() - d.getTime()) / 1000);
  if (sec < 0) return 'только что';
  if (sec < 60) return `${sec} с назад`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min} мин назад`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr} ч назад`;
  const day = Math.floor(hr / 24);
  if (day < 30) return `${day} дн назад`;
  return fmtDate(v, false);
}

export function fmtDuration(sec) {
  if (sec == null || isNaN(sec)) return '—';
  sec = Math.round(sec);
  if (sec < 60) return `${sec}с`;
  const m = Math.floor(sec / 60), s = sec % 60;
  if (m < 60) return `${m}м ${s}с`;
  const h = Math.floor(m / 60);
  return `${h}ч ${m % 60}м`;
}

export function fmtBytes(n) {
  if (n == null || isNaN(n)) return '—';
  const u = ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ'];
  let i = 0; n = Number(n);
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(i ? 1 : 0)} ${u[i]}`;
}

export function fmtNum(n) {
  if (n == null || isNaN(n)) return '—';
  return Number(n).toLocaleString('ru-RU');
}

export function fmtPct(n, digits = 0) {
  if (n == null || isNaN(n)) return '—';
  return `${(Number(n) * 100).toFixed(digits)}%`;
}

export function truncate(s, n = 80) {
  if (s == null) return '';
  s = String(s);
  return s.length > n ? s.slice(0, n - 1) + '…' : s;
}

// Маппинг статусов пайплайна → вариант бейджа.
export function statusVariant(status) {
  const s = String(status || '').toLowerCase();
  if (['done', 'ok', 'success', 'indexed', 'active', 'healthy', 'assembled'].includes(s)) return 'success';
  if (['error', 'failed', 'dropped', 'down', 'unhealthy'].includes(s)) return 'danger';
  if (['pending', 'queued', 'processing', 'running', 'degraded'].includes(s)) return 'warn';
  if (['skipped', 'disabled', 'inactive', 'not_called', 'empty'].includes(s)) return 'neutral';
  return 'neutral';
}
