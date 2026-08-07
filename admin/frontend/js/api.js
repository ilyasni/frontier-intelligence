// Единый слой доступа к API. Same-origin (Basic-auth подставляет браузер).
// Все ошибки нормализуются в ApiError — вызывающий делает try/catch и показывает error-состояние.

export class ApiError extends Error {
  constructor(status, detail, body) {
    super(detail || `HTTP ${status}`);
    this.name = 'ApiError';
    this.status = status;
    this.detail = detail;
    this.body = body;
  }
}

async function request(method, path, { params, body } = {}) {
  let url = path;
  if (params) {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null && v !== '') qs.append(k, v);
    }
    const s = qs.toString();
    if (s) url += (url.includes('?') ? '&' : '?') + s;
  }
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }

  let resp;
  try {
    resp = await fetch(url, opts);
  } catch (e) {
    throw new ApiError(0, 'Сеть недоступна — сервер не отвечает');
  }

  const text = await resp.text();
  let data = null;
  if (text) {
    try { data = JSON.parse(text); } catch { data = text; }
  }

  if (!resp.ok) {
    let detail = (data && typeof data === 'object' && (data.detail || data.error || data.message)) || resp.statusText || `HTTP ${resp.status}`;
    if (typeof detail !== 'string') detail = JSON.stringify(detail);
    if (resp.status === 401) {
      // 401 бывает двух разных видов, и раньше они схлопывались в один текст.
      //
      // (1) Middleware на защищённом /api/*: тело `{"detail": "unauthorized"}`,
      //     смысл — «сессия истекла, войди заново». Здесь подмена уместна.
      // (2) Сам обработчик логина: тело `{"detail": "Неверный логин или пароль"}`
      //     (admin/backend/main.py — эндпоинт вынесен из middleware). Раньше этот
      //     текст безусловно затирался, и форма входа на опечатку в пароле отвечала
      //     «Требуется вход» — то есть единственный осмысленный диагноз стирался
      //     ровно там, где он нужен.
      //
      // Событие для показа формы входа шлём только в случае (1): на самом экране
      // входа оно бессмысленно.
      const isLoginAttempt = path === '/api/auth/login';
      if (!isLoginAttempt) {
        window.dispatchEvent(new CustomEvent('admin-unauthorized'));
      }
      if (!detail || detail === 'unauthorized' || detail === `HTTP ${resp.status}`) {
        detail = 'Требуется вход';
      }
    }
    throw new ApiError(resp.status, detail, data);
  }
  return data;
}

export const api = {
  get: (p, params) => request('GET', p, { params }),
  post: (p, body, params) => request('POST', p, { body, params }),
  patch: (p, body) => request('PATCH', p, { body }),
  del: (p) => request('DELETE', p),
};
