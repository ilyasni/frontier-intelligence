// Тосты и confirm-диалог (замена alert/confirm/prompt). Реактивные глобальные сторы.
const { reactive } = window.Vue;

export const toastState = reactive({ items: [] });
let _tid = 0;

export function toast(variant, title, msg, timeout = 4500) {
  const id = ++_tid;
  toastState.items.push({ id, variant, title, msg });
  if (timeout) setTimeout(() => dismissToast(id), timeout);
  return id;
}
export function dismissToast(id) {
  const i = toastState.items.findIndex((t) => t.id === id);
  if (i >= 0) toastState.items.splice(i, 1);
}
export const notify = {
  success: (t, m) => toast('success', t, m),
  error: (t, m) => toast('error', t, m, 8000),
  warn: (t, m) => toast('warn', t, m, 6000),
  info: (t, m) => toast('info', t, m),
};

// Единый confirm-диалог вместо window.confirm.
export const confirmState = reactive({
  open: false, title: '', message: '', confirmText: 'Подтвердить', danger: false, _resolve: null,
});
export function confirmDialog({ title, message, confirmText = 'Подтвердить', danger = false }) {
  return new Promise((resolve) => {
    Object.assign(confirmState, { open: true, title, message, confirmText, danger, _resolve: resolve });
  });
}
export function resolveConfirm(value) {
  confirmState.open = false;
  const r = confirmState._resolve;
  confirmState._resolve = null;
  if (r) r(value);
}
