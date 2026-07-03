import { api } from '../../api.js';
import SettingCard from './SettingCard.js';
import KvGrid from './KvGrid.js';
import { fmtCompact, compactMiddle, secretIsCritical } from './helpers.js';

// Вкладка «Система»: integrations, business rules, redis streams, secret flags (только булевы *_set).
export default {
  name: 'SystemTab',
  components: { SettingCard, KvGrid },
  data() {
    return {
      settings: { data: null, loading: true, error: null },
      streams: { data: null, loading: true, error: null },
    };
  },
  computed: {
    d() { return this.settings.data || {}; },
    runtime() { return this.d.runtime || {}; },
    business() { return this.d.business || {}; },
    integrations() { return this.d.integrations || {}; },
    secrets() { return this.d.secrets || {}; },
    integrationRows() {
      return Object.entries(this.integrations).map(([key, value]) => ({ key, value }));
    },
    businessCells() {
      const b = this.business; const r = this.runtime;
      return [
        { label: 'Порог релевантности', value: String(b.default_relevance_threshold != null ? b.default_relevance_threshold : '—'), meta: 'Дефолтный cutoff done vs dropped' },
        { label: 'Missing-signals min gap', value: String(r.missing_signals_min_gap_score != null ? r.missing_signals_min_gap_score : '—'), meta: 'Когда воркер отмечает пробел в сигналах' },
        { label: 'Missing-signals topic cap', value: String(r.missing_signals_topic_limit != null ? r.missing_signals_topic_limit : '—'), meta: 'Лимит grounded-топиков за прогон' },
        { label: 'SearXNG max results', value: String(r.searxng_max_results != null ? r.searxng_max_results : '—'), meta: 'Сколько внешних свидетельств за шаг' },
      ];
    },
    criticalSecrets() { return Object.entries(this.secrets).filter(([k]) => secretIsCritical(k)).map(([k, v]) => ({ k, v: !!v })); },
    optionalSecrets() { return Object.entries(this.secrets).filter(([k]) => !secretIsCritical(k)).map(([k, v]) => ({ k, v: !!v })); },
    missingCritical() { return this.criticalSecrets.filter((s) => !s.v).map((s) => s.k); },
    secretsConfigured() { return Object.values(this.secrets).filter(Boolean).length; },
    secretsTotal() { return Object.keys(this.secrets).length; },
    streamRows() { return (this.streams.data && Array.isArray(this.streams.data.streams)) ? this.streams.data.streams : []; },
    streamsBacklog() {
      return this.streamRows().some((s) => Number(s.lag || 0) > 0 || Number(s.pending || 0) > 0 || Number(s.oldest_pending_age_seconds || 0) > 0);
    },
  },
  mounted() { this.loadAll(); },
  methods: {
    fmtCompact, compactMiddle,
    async loadSection(key, path) {
      this[key] = { data: this[key].data, loading: true, error: null };
      try { this[key] = { data: await api.get(path), loading: false, error: null }; }
      catch (e) { this[key] = { data: null, loading: false, error: e }; }
    },
    loadAll() {
      this.loadSection('settings', '/api/settings');
      this.loadSection('streams', '/api/pipeline/streams');
    },
  },
  template: `
  <div class="stack" style="gap:var(--sp-4)">

    <!-- Business rules -->
    <SettingCard title="Бизнес-правила"
      desc="Workspace-agnostic дефолты релевантности и поведения кластеризации."
      badge-text="Configured" badge-variant="success"
      :loading="settings.loading" :error="settings.error"
      @retry="loadSection('settings', '/api/settings')">
      <div class="stack" style="gap:var(--sp-4)">
        <KvGrid :items="businessCells"/>
        <details><summary class="muted text-sm" style="cursor:pointer">Raw business config</summary><div class="mt-2"><JsonView :data="business"/></div></details>
      </div>
    </SettingCard>

    <!-- Integrations -->
    <SettingCard title="Интеграции"
      desc="Внешние эндпоинты, прокси-маршруты и строки подключения сервисов."
      :badge-text="integrationRows.length ? integrationRows.length + ' записей' : ''" badge-variant="neutral"
      :loading="settings.loading" :error="settings.error"
      :empty="!integrationRows.length && !settings.loading" empty-text="Интеграции не заданы"
      @retry="loadSection('settings', '/api/settings')">
      <div class="table-wrap">
        <table class="tbl tbl--fixed" style="min-width:560px">
          <colgroup><col style="width:34%"><col style="width:66%"></colgroup>
          <thead><tr><th>Ключ</th><th>Значение</th></tr></thead>
          <tbody>
            <tr v-for="(it, i) in integrationRows" :key="i">
              <td class="mono">{{ it.key }}</td>
              <td class="mono muted ellip" :title="String(it.value)">{{ compactMiddle(it.value, 60) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <details class="mt-4"><summary class="muted text-sm" style="cursor:pointer">Raw integrations</summary><div class="mt-2"><JsonView :data="integrations"/></div></details>
    </SettingCard>

    <!-- Redis streams -->
    <SettingCard title="Redis stream-очереди"
      desc="Backlog и pending из XINFO GROUPS и XPENDING."
      :badge-text="streamRows().length ? (streamsBacklog ? 'Backlog' : 'Healthy') : ''"
      :badge-variant="streamRows().length ? (streamsBacklog ? 'warn' : 'success') : 'neutral'"
      :loading="streams.loading" :error="streams.error"
      :empty="!streamRows().length && !streams.loading" empty-text="Стримов не найдено"
      @retry="loadSection('streams', '/api/pipeline/streams')">
      <div class="table-wrap">
        <table class="tbl tbl--fixed" style="min-width:620px">
          <thead><tr><th>Stream</th><th>Group</th><th class="num">Lag</th><th class="num">Pending</th><th class="num">Oldest pending</th></tr></thead>
          <tbody>
            <tr v-for="(s, i) in streamRows()" :key="i">
              <td class="mono ellip" :title="s.stream">{{ s.stream || '—' }}</td>
              <td class="mono ellip" :title="s.group">{{ s.group || '—' }}</td>
              <td class="num">{{ fmtCompact(s.lag || 0) }}</td>
              <td class="num">{{ fmtCompact(s.pending || 0) }}</td>
              <td class="num">{{ Math.round(Number(s.oldest_pending_age_seconds || 0)) }}с</td>
            </tr>
          </tbody>
        </table>
      </div>
      <details class="mt-4"><summary class="muted text-sm" style="cursor:pointer">Raw stream snapshot</summary><div class="mt-2"><JsonView :data="streams.data"/></div></details>
    </SettingCard>

    <!-- Secret flags -->
    <SettingCard title="Флаги секретов"
      desc="Булевый чеклист — какие креды подключены (значения секретов НЕ показываются)."
      :badge-text="missingCritical.length ? missingCritical.length + ' критичных нет' : secretsConfigured + '/' + secretsTotal + ' задано'"
      :badge-variant="missingCritical.length ? 'danger' : (secretsConfigured === secretsTotal && secretsTotal ? 'success' : 'warn')"
      :loading="settings.loading" :error="settings.error"
      :empty="!secretsTotal && !settings.loading" empty-text="Флагов секретов нет"
      @retry="loadSection('settings', '/api/settings')">
      <div class="stack" style="gap:var(--sp-4)">
        <div v-if="missingCritical.length" class="badge badge--danger" style="display:block;padding:10px 12px;border-radius:var(--r-md)">
          Критичные не заданы: {{ missingCritical.join(', ') }}
        </div>
        <div>
          <div class="muted text-sm mb-3">Критичные</div>
          <div class="row row--wrap" style="gap:var(--sp-2)">
            <UiBadge v-for="s in criticalSecrets" :key="s.k" :variant="s.v ? 'success' : 'danger'" :text="s.k + ': ' + (s.v ? 'set' : 'missing')" :dot="false"/>
          </div>
        </div>
        <div>
          <div class="muted text-sm mb-3">Опциональные</div>
          <div class="row row--wrap" style="gap:var(--sp-2)">
            <UiBadge v-for="s in optionalSecrets" :key="s.k" :variant="s.v ? 'success' : 'warn'" :text="s.k + ': ' + (s.v ? 'set' : 'missing')" :dot="false"/>
          </div>
        </div>
        <details><summary class="muted text-sm" style="cursor:pointer">Raw secret flags</summary><div class="mt-2"><JsonView :data="secrets"/></div></details>
      </div>
    </SettingCard>

  </div>
  `,
};
