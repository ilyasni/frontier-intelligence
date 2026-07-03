// Точка входа. Vue и Vue Router загружены как глобальные скрипты (vendor) до этого модуля.
const { createApp } = window.Vue;
const { createRouter, createWebHashHistory } = window.VueRouter;

import AppLayout from './components/AppLayout.js';
import UiBadge from './components/UiBadge.js';
import StateBlock from './components/StateBlock.js';
import UiModal from './components/UiModal.js';
import JsonView from './components/JsonView.js';
import WorkspaceSelect from './components/WorkspaceSelect.js';

import DashboardView from './views/DashboardView.js';
import PipelineView from './views/PipelineView.js';
import SourcesView from './views/SourcesView.js';
import PostsView from './views/PostsView.js';
import AlbumsView from './views/AlbumsView.js';
import MediaView from './views/MediaView.js';
import ClustersView from './views/ClustersView.js';
import GraphView from './views/GraphView.js';
import SearchView from './views/SearchView.js';
import WorkspacesView from './views/WorkspacesView.js';
import SettingsView from './views/SettingsView.js';

const routes = [
  { path: '/', component: DashboardView, meta: { title: 'Dashboard' } },
  { path: '/pipeline', component: PipelineView, meta: { title: 'Pipeline' } },
  { path: '/sources', component: SourcesView, meta: { title: 'Источники' } },
  { path: '/posts', component: PostsView, meta: { title: 'Посты' } },
  { path: '/albums', component: AlbumsView, meta: { title: 'Альбомы' } },
  { path: '/media', component: MediaView, meta: { title: 'Медиа' } },
  { path: '/clusters', component: ClustersView, meta: { title: 'Кластеры' } },
  { path: '/graph', component: GraphView, meta: { title: 'Граф' } },
  { path: '/search', component: SearchView, meta: { title: 'Поиск' } },
  { path: '/workspaces', component: WorkspacesView, meta: { title: 'Workspaces' } },
  { path: '/settings', component: SettingsView, meta: { title: 'Настройки' } },
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
