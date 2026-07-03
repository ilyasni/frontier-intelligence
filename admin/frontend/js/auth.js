// Состояние авторизации (cookie-сессия). Гейтит приложение до входа.
const { reactive } = window.Vue;
import { api } from './api.js';

export const auth = reactive({ ready: false, authed: false, user: null });

export async function checkAuth() {
  try {
    const r = await api.get('/api/auth/me');
    auth.authed = true; auth.user = r.user;
  } catch {
    auth.authed = false; auth.user = null;
  } finally {
    auth.ready = true;
  }
}

export async function login(username, password) {
  const r = await api.post('/api/auth/login', { username, password });
  auth.authed = true; auth.user = r.user;
  return r;
}

export async function logout() {
  try { await api.post('/api/auth/logout'); } catch { /* ignore */ }
  auth.authed = false; auth.user = null;
}

// api.js диспатчит это событие при 401 — сессия истекла посреди работы.
window.addEventListener('admin-unauthorized', () => { auth.authed = false; });
