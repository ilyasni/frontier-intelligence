// Точка входа. Vue и Vue Router загружены как глобальные скрипты (vendor) до этого модуля.
const { createApp } = window.Vue;
const { createRouter, createWebHashHistory } = window.VueRouter;

import AppLayout from './components/AppLayout.js';
import UiBadge from './components/UiBadge.js';
import StateBlock from './components/StateBlock.js';
import UiModal from './components/UiModal.js';
import JsonView from './components/JsonView.js';
import WorkspaceSelect from './components/WorkspaceSelect.js';

// Ленивая загрузка экранов: каждый грузится при переходе. Ошибка в одном экране
// изолируется этим маршрутом и не роняет всё приложение.
const routes = [
  { path: '/', component: () => import('./views/DashboardView.js'), meta: { title: 'Dashboard' } },
  { path: '/pipeline', component: () => import('./views/PipelineView.js'), meta: { title: 'Pipeline' } },
  { path: '/sources', component: () => import('./views/SourcesView.js'), meta: { title: 'Источники' } },
  { path: '/posts', component: () => import('./views/PostsView.js'), meta: { title: 'Посты' } },
  { path: '/albums', component: () => import('./views/AlbumsView.js'), meta: { title: 'Альбомы' } },
  { path: '/media', component: () => import('./views/MediaView.js'), meta: { title: 'Медиа' } },
  { path: '/clusters', component: () => import('./views/ClustersView.js'), meta: { title: 'Кластеры' } },
  { path: '/graph', component: () => import('./views/GraphView.js'), meta: { title: 'Граф' } },
  { path: '/search', component: () => import('./views/SearchView.js'), meta: { title: 'Поиск' } },
  { path: '/workspaces', component: () => import('./views/WorkspacesView.js'), meta: { title: 'Workspaces' } },
  { path: '/settings', component: () => import('./views/SettingsView.js'), meta: { title: 'Настройки' } },
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
