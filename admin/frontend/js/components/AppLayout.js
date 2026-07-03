import UiToast from './UiToast.js';
import ConfirmDialog from './ConfirmDialog.js';
import LoginView from '../views/LoginView.js';
import { auth, checkAuth, logout } from '../auth.js';

const NAV = [
  { section: 'Обзор', items: [
    { to: '/', icon: '▦', label: 'Dashboard' },
    { to: '/pipeline', icon: '⛓', label: 'Pipeline' },
  ] },
  { section: 'Данные', items: [
    { to: '/sources', icon: '📡', label: 'Источники' },
    { to: '/posts', icon: '📄', label: 'Посты' },
    { to: '/albums', icon: '🖼', label: 'Альбомы' },
    { to: '/media', icon: '🎞', label: 'Медиа' },
  ] },
  { section: 'Аналитика', items: [
    { to: '/clusters', icon: '◈', label: 'Кластеры' },
    { to: '/graph', icon: '⬡', label: 'Граф' },
    { to: '/search', icon: '⚲', label: 'Поиск' },
  ] },
  { section: 'Система', items: [
    { to: '/workspaces', icon: '▤', label: 'Workspaces' },
    { to: '/settings', icon: '⚙', label: 'Настройки' },
  ] },
];

export default {
  name: 'AppLayout',
  components: { UiToast, ConfirmDialog, LoginView },
  data() { return { nav: NAV, sidebarOpen: false }; },
  computed: {
    auth() { return auth; },
    title() { return (this.$route.meta && this.$route.meta.title) || 'Frontier Admin'; },
  },
  watch: { $route() { this.sidebarOpen = false; } },
  mounted() { checkAuth(); },
  methods: {
    async doLogout() { await logout(); },
  },
  template: `
    <div class="app">
      <div v-if="!auth.ready" style="min-height:100vh;display:flex;align-items:center;justify-content:center">
        <div class="spin" style="width:22px;height:22px"></div>
      </div>

      <LoginView v-else-if="!auth.authed" @success="() => {}"/>

      <template v-else>
        <aside class="sidebar" :class="sidebarOpen && 'open'">
          <div class="sidebar__brand"><span class="dot"></span> Frontier</div>
          <nav class="sidebar__nav">
            <template v-for="grp in nav" :key="grp.section">
              <div class="sidebar__section">{{ grp.section }}</div>
              <router-link v-for="it in grp.items" :key="it.to" :to="it.to" class="nav-link">
                <span class="ico">{{ it.icon }}</span>{{ it.label }}
              </router-link>
            </template>
          </nav>
        </aside>
        <div class="main">
          <header class="topbar">
            <button class="btn btn--icon btn--ghost hamburger" @click="sidebarOpen = !sidebarOpen" aria-label="Меню">☰</button>
            <div class="topbar__title">{{ title }}</div>
            <div class="topbar__spacer"></div>
            <span class="text-xs faint">{{ auth.user }}</span>
            <button class="btn btn--outline btn--sm" @click="doLogout">Выйти</button>
          </header>
          <main class="content"><router-view/></main>
        </div>
      </template>

      <UiToast/>
      <ConfirmDialog/>
    </div>
  `,
};
