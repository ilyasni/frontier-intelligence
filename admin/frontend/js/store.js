// Общий реактивный стор. Список workspaces грузится один раз и переиспользуется
// всеми экранами как фильтр (workspace_id).
const { reactive } = window.Vue;
import { api } from './api.js';

export const store = reactive({
  workspaces: [],
  workspacesLoaded: false,
});

export async function loadWorkspaces(force = false) {
  if (store.workspacesLoaded && !force) return store.workspaces;
  const ws = await api.get('/api/workspaces');
  store.workspaces = Array.isArray(ws) ? ws : [];
  store.workspacesLoaded = true;
  return store.workspaces;
}
