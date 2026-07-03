// Точка входа. Vue и Vue Router загружены как глобальные скрипты (vendor) до этого модуля.
const { createApp } = window.Vue;
const { createRouter, createWebHashHistory } = window.VueRouter;

import AppLayout from './components/AppLayout.js';
import UiBadge from './components/UiBadge.js';
import StateBlock from './components/StateBlock.js';
import UiModal from './components/UiModal.js';
import JsonView from './components/JsonView.js';
import WorkspaceSelect from './components/WorkspaceSelect.js';

import Placeholder from './views/Placeholder.js';
import SourcesView from './views/SourcesView.js';

const routes = [
  { path: '/', component: Placeholder, meta: { title: 'Dashboard' } },
  { path: '/pipeline', component: Placeholder, meta: { title: 'Pipeline' } },
  { path: '/sources', component: SourcesView, meta: { title: 'Источники' } },
  { path: '/posts', component: Placeholder, meta: { title: 'Посты' } },
  { path: '/albums', component: Placeholder, meta: { title: 'Альбомы' } },
  { path: '/media', component: Placeholder, meta: { title: 'Медиа' } },
  { path: '/clusters', component: Placeholder, meta: { title: 'Кластеры' } },
  { path: '/graph', component: Placeholder, meta: { title: 'Граф' } },
  { path: '/search', component: Placeholder, meta: { title: 'Поиск' } },
  { path: '/workspaces', component: Placeholder, meta: { title: 'Workspaces' } },
  { path: '/settings', component: Placeholder, meta: { title: 'Настройки' } },
  { path: '/:pathMatch(.*)*', redirect: '/' },
];

const router = createRouter({ history: createWebHashHistory(), routes });

const app = createApp(AppLayout);
app.component('UiBadge', UiBadge);
app.component('StateBlock', StateBlock);
app.component('UiModal', UiModal);
app.component('JsonView', JsonView);
app.component('WorkspaceSelect', WorkspaceSelect);
app.use(router);

app.config.errorHandler = (err) => { console.error('[admin]', err); };

app.mount('#app');
