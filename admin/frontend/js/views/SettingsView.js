import RuntimeRoutingTab from '../components/settings/RuntimeRoutingTab.js';
import ProvidersTab from '../components/settings/ProvidersTab.js';
import XrayTab from '../components/settings/XrayTab.js';
import SystemTab from '../components/settings/SystemTab.js';

// Settings / control-plane / FinOps / xray — самый большой экран админки.
// Организован во вкладки, чтобы не скроллить «простыню» из ~18 панелей.
// Данные грузятся лениво при первом открытии вкладки; <keep-alive> хранит состояние,
// поэтому повторный вход не перезапрашивает. Каждая секция внутри вкладки — свой try/catch + StateBlock.

const TABS = [
  { id: 'runtime', label: 'Runtime и роутинг', comp: 'RuntimeRoutingTab', hint: 'runtime-mode · политика v2 · симулятор' },
  { id: 'providers', label: 'Провайдеры и FinOps', comp: 'ProvidersTab', hint: 'provider-state · бюджеты · circuits · Wormsoft/OpenRouter/GigaChat' },
  { id: 'xray', label: 'Xray', comp: 'XrayTab', hint: 'egress health · профили · ремедиация' },
  { id: 'system', label: 'Система', comp: 'SystemTab', hint: 'интеграции · бизнес-правила · Redis · секреты' },
];

export default {
  name: 'SettingsView',
  components: { RuntimeRoutingTab, ProvidersTab, XrayTab, SystemTab },
  data() {
    const q = this.$route.query.tab;
    const valid = TABS.some((t) => t.id === q);
    return {
      // <component :is> монтирует только активную вкладку (ленивый маунт),
      // <keep-alive> кэширует уже открытые — повторный вход не перезапрашивает.
      activeTab: valid ? q : 'runtime',
    };
  },
  computed: {
    tabs() { return TABS; },
    activeComp() { return (TABS.find((t) => t.id === this.activeTab) || TABS[0]).comp; },
  },
  watch: {
    activeTab(v) {
      if (this.$route.query.tab !== v) {
        this.$router.replace({ query: { ...this.$route.query, tab: v } });
      }
    },
  },
  methods: {
    select(id) { this.activeTab = id; },
    // Стиль таба через токены (не трогаем css). Активный — акцентная подложка + нижняя граница.
    tabStyle(id) {
      const active = this.activeTab === id;
      return {
        padding: '9px 16px',
        border: '1px solid ' + (active ? 'var(--accent-border)' : 'transparent'),
        borderBottom: active ? '2px solid var(--accent)' : '2px solid transparent',
        borderRadius: 'var(--r-md) var(--r-md) 0 0',
        background: active ? 'var(--accent-soft)' : 'transparent',
        color: active ? 'var(--text)' : 'var(--text-muted)',
        fontSize: 'var(--fs-sm)',
        fontWeight: active ? '650' : '550',
        cursor: 'pointer',
        whiteSpace: 'nowrap',
      };
    },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Настройки</h1>
        <p>Control-plane роутинга, FinOps, egress-контур и системная конфигурация.</p>
      </div>
    </div>

    <div role="tablist" aria-label="Разделы настроек"
      style="display:flex;gap:var(--sp-1);flex-wrap:wrap;border-bottom:1px solid var(--border);margin-bottom:var(--sp-5)">
      <button v-for="t in tabs" :key="t.id" type="button" role="tab" :aria-selected="activeTab === t.id ? 'true' : 'false'"
        :style="tabStyle(t.id)" @click="select(t.id)" :title="t.hint">
        {{ t.label }}
      </button>
    </div>

    <div>
      <keep-alive>
        <component :is="activeComp"/>
      </keep-alive>
    </div>
  </div>
  `,
};
