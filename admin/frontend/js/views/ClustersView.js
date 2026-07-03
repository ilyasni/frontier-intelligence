import { api } from '../api.js';
import { notify } from '../ui.js';
import { fmtAgo, fmtDate, fmtNum, statusVariant, truncate } from '../format.js';

// Вкладки страницы аналитики трендов. У каждой — свой эндпоинт и таблица ключевых полей;
// полный объект строки показываем в модалке (dl + JsonView). Стадии emerging — чипами.
const TABS = [
  { id: 'semantic', label: 'Семантические' },
  { id: 'trends', label: 'Тренды' },
  { id: 'emerging', label: 'Emerging' },
  { id: 'missing', label: 'Missing' },
  { id: 'runs', label: 'Прогоны' },
];

const STAGES = [
  { v: 'weak', l: 'weak' },
  { v: 'emerging', l: 'emerging' },
  { v: 'stable', l: 'stable' },
  { v: 'fading', l: 'fading' },
];

// entity_kind для /api/clusters/timeline — только для кластерных сущностей.
const TIMELINE_KIND = { semantic: 'semantic', trends: 'trend', emerging: 'emerging' };

const num = (v) => (v == null || isNaN(v) ? '—' : Number(v).toFixed(2));

export default {
  name: 'ClustersView',
  data() {
    return {
      activeTab: 'semantic',
      filters: { workspace: this.$route.query.ws || '', limit: 50 },
      stages: ['emerging'], // выбранные стадии для вкладки emerging
      trendPipeline: 'stable',
      // по вкладкам: свои данные/состояния, чтобы переключение не сбрасывало соседние
      rows: { semantic: [], trends: [], emerging: [], missing: [], runs: [] },
      loading: false,
      error: null,
      loaded: {},
      detail: null, // выбранная строка (полный объект)
      timeline: null, // { cluster, series } для detail, если применимо
      timelineErr: '',
    };
  },
  computed: {
    currentRows() { return this.rows[this.activeTab] || []; },
    emptyText() {
      const m = {
        semantic: 'Семантических кластеров пока нет',
        trends: 'Трендовых кластеров пока нет',
        emerging: 'Emerging-сигналов пока нет',
        missing: 'Missing-сигналов пока нет',
        runs: 'Прогонов пока нет',
      };
      return m[this.activeTab] || 'Ничего не найдено';
    },
    tabs() { return TABS; },
    stageOptions() { return STAGES; },
  },
  watch: {
    'filters.workspace'(v) {
      this.$router.replace({ query: { ...this.$route.query, ws: v || undefined } });
      this.reloadAll();
    },
  },
  mounted() { this.load(); },
  methods: {
    fmtAgo, fmtDate, fmtNum, statusVariant, truncate, num,
    tabLabel(id) { return (TABS.find((t) => t.id === id) || {}).label || id; },
    switchTab(id) {
      this.activeTab = id;
      if (!this.loaded[id]) this.load();
    },
    // Сбросить кэш всех вкладок и перегрузить активную (после смены workspace).
    reloadAll() {
      this.loaded = {};
      this.load();
    },
    toggleStage(v) {
      const i = this.stages.indexOf(v);
      if (i >= 0) this.stages.splice(i, 1);
      else this.stages.push(v);
      this.loaded.emerging = false;
      if (this.activeTab === 'emerging') this.load();
    },
    isStageOn(v) { return this.stages.includes(v); },
    async load() {
      const tab = this.activeTab;
      this.loading = true; this.error = null;
      try {
        const rows = await this.fetchTab(tab);
        this.rows[tab] = Array.isArray(rows) ? rows : [];
        this.loaded[tab] = true;
      } catch (e) { this.error = e; }
      finally { this.loading = false; }
    },
    fetchTab(tab) {
      const ws = this.filters.workspace || undefined;
      const limit = Number(this.filters.limit) || 50;
      if (tab === 'semantic') return api.get('/api/clusters/semantic', { workspace_id: ws, limit });
      if (tab === 'trends') return api.get('/api/clusters/trends', { workspace_id: ws, pipeline: this.trendPipeline || undefined, limit });
      if (tab === 'emerging') {
        // stages[] — повторяющийся параметр; URLSearchParams в api.js не умеет массивы,
        // поэтому собираем query вручную.
        const qs = new URLSearchParams();
        if (ws) qs.set('workspace_id', ws);
        qs.set('limit', String(limit));
        (this.stages.length ? this.stages : ['emerging']).forEach((s) => qs.append('stages', s));
        return api.get('/api/clusters/emerging?' + qs.toString());
      }
      if (tab === 'missing') return api.get('/api/clusters/missing', { workspace_id: ws, limit });
      if (tab === 'runs') return api.get('/api/clusters/runs', { workspace_id: ws, limit });
      return Promise.resolve([]);
    },
    // Клик по строке: полный объект в модалку + попытка подтянуть timeline для кластерных вкладок.
    async openDetail(row) {
      this.detail = row;
      this.timeline = null; this.timelineErr = '';
      const kind = TIMELINE_KIND[this.activeTab];
      const id = row.id ?? row.cluster_id ?? row.signal_id;
      if (!kind || id == null) return;
      try {
        this.timeline = await api.get('/api/clusters/timeline', {
          entity_kind: kind, entity_id: id, workspace_id: this.filters.workspace || undefined,
        });
      } catch (e) { this.timelineErr = e.detail || 'Не удалось загрузить timeline'; }
    },
    closeDetail() { this.detail = null; this.timeline = null; this.timelineErr = ''; },
    detailTitle(r) { return (r && (r.title || r.label || r.name)) || 'Кластер #' + (r && (r.id ?? r.cluster_id ?? r.signal_id)); },
    // Значения для series-мини-таблицы timeline.
    seriesRows() {
      const s = this.timeline && this.timeline.series;
      return Array.isArray(s) ? s : [];
    },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Кластеры и тренды</h1>
        <p>Аналитика сигналов: семантические и трендовые кластеры, emerging и missing, история прогонов</p>
      </div>
      <div class="page-head__actions">
        <button class="btn btn--outline" @click="load" :disabled="loading">↻ Обновить</button>
      </div>
    </div>

    <div class="toolbar">
      <div class="toolbar__group"><label>Workspace</label><WorkspaceSelect v-model="filters.workspace"/></div>
      <div class="toolbar__group"><label>Limit</label>
        <input class="input" type="number" min="1" max="500" v-model.number="filters.limit" @change="reloadAll" style="min-width:100px">
      </div>
      <div v-if="activeTab==='trends'" class="toolbar__group"><label>Pipeline</label>
        <select class="select" v-model="trendPipeline" @change="load">
          <option value="stable">stable</option>
          <option value="emerging">emerging</option>
          <option value="">все</option>
        </select>
      </div>
    </div>

    <!-- Вкладки -->
    <div class="row row--wrap" style="gap:var(--sp-2);margin-bottom:var(--sp-4)">
      <button v-for="t in tabs" :key="t.id" class="btn btn--sm"
        :class="activeTab===t.id ? 'btn--primary' : 'btn--outline'" @click="switchTab(t.id)">
        {{ t.label }}
      </button>
    </div>

    <!-- Чипы стадий (emerging) -->
    <div v-if="activeTab==='emerging'" class="row row--wrap" style="gap:var(--sp-2);margin-bottom:var(--sp-4)">
      <span class="text-xs faint" style="align-self:center">Стадии:</span>
      <button v-for="s in stageOptions" :key="s.v" class="btn btn--sm"
        :class="isStageOn(s.v) ? 'btn--primary' : 'btn--outline'" @click="toggleStage(s.v)">
        {{ s.l }}
      </button>
    </div>

    <div class="card"><div class="card__body--flush">
      <StateBlock :loading="loading" :error="error" :empty="!currentRows.length" :empty-text="emptyText" @retry="load">

        <!-- Семантические -->
        <div v-if="activeTab==='semantic'" class="table-wrap">
          <table class="tbl tbl--fixed">
            <colgroup><col style="width:38%"><col style="width:20%"><col style="width:12%"><col style="width:14%"><col style="width:16%"></colgroup>
            <thead><tr><th>Кластер</th><th>Workspace</th><th class="num">Постов</th><th>Состояние</th><th>Обнаружен</th></tr></thead>
            <tbody>
              <tr v-for="c in currentRows" :key="c.id ?? c.cluster_id" style="cursor:pointer" @click="openDetail(c)">
                <td><div class="ellip" style="font-weight:600" :title="c.title || c.label">{{ c.title || c.label || '—' }}</div></td>
                <td class="muted"><span class="ellip mono text-xs" style="display:block" :title="c.workspace_id">{{ c.workspace_id || '—' }}</span></td>
                <td class="num mono">{{ fmtNum(c.post_count ?? c.doc_count) }}</td>
                <td><UiBadge :variant="statusVariant(c.lifecycle_state || 'active')" :text="c.lifecycle_state || 'active'" :dot="false"/></td>
                <td class="muted text-sm">{{ c.detected_at ? fmtAgo(c.detected_at) : '—' }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Тренды -->
        <div v-else-if="activeTab==='trends'" class="table-wrap">
          <table class="tbl tbl--fixed">
            <colgroup><col style="width:32%"><col style="width:13%"><col style="width:13%"><col style="width:13%"><col style="width:13%"><col style="width:16%"></colgroup>
            <thead><tr><th>Кластер</th><th>Pipeline</th><th class="num">Signal</th><th class="num">Burst</th><th>Стадия</th><th>Обнаружен</th></tr></thead>
            <tbody>
              <tr v-for="c in currentRows" :key="c.id ?? c.cluster_id" style="cursor:pointer" @click="openDetail(c)">
                <td><div class="ellip" style="font-weight:600" :title="c.title || c.label">{{ c.title || c.label || '—' }}</div></td>
                <td><span class="ellip mono text-xs" style="display:block" :title="c.pipeline">{{ c.pipeline || '—' }}</span></td>
                <td class="num mono">{{ num(c.signal_score) }}</td>
                <td class="num mono">{{ num(c.burst_score) }}</td>
                <td><UiBadge v-if="c.signal_stage" :variant="statusVariant(c.signal_stage)" :text="c.signal_stage" :dot="false"/><span v-else class="faint">—</span></td>
                <td class="muted text-sm">{{ c.detected_at ? fmtAgo(c.detected_at) : '—' }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Emerging -->
        <div v-else-if="activeTab==='emerging'" class="table-wrap">
          <table class="tbl tbl--fixed">
            <colgroup><col style="width:38%"><col style="width:18%"><col style="width:15%"><col style="width:14%"><col style="width:15%"></colgroup>
            <thead><tr><th>Сигнал</th><th>Workspace</th><th>Стадия</th><th class="num">Signal</th><th class="num">Confidence</th></tr></thead>
            <tbody>
              <tr v-for="s in currentRows" :key="s.id ?? s.signal_id" style="cursor:pointer" @click="openDetail(s)">
                <td><div class="ellip" style="font-weight:600" :title="s.title || s.label">{{ s.title || s.label || '—' }}</div></td>
                <td class="muted"><span class="ellip mono text-xs" style="display:block" :title="s.workspace_id">{{ s.workspace_id || '—' }}</span></td>
                <td><UiBadge :variant="statusVariant(s.signal_stage || 'weak')" :text="s.signal_stage || 'weak'" :dot="false"/></td>
                <td class="num mono">{{ num(s.signal_score) }}</td>
                <td class="num mono">{{ num(s.confidence) }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Missing -->
        <div v-else-if="activeTab==='missing'" class="table-wrap">
          <table class="tbl tbl--fixed">
            <colgroup><col style="width:44%"><col style="width:22%"><col style="width:16%"><col style="width:18%"></colgroup>
            <thead><tr><th>Пропущенный сигнал</th><th>Workspace</th><th class="num">Gap score</th><th>Обнаружен</th></tr></thead>
            <tbody>
              <tr v-for="m in currentRows" :key="m.id ?? m.signal_id" style="cursor:pointer" @click="openDetail(m)">
                <td><div class="ellip" style="font-weight:600" :title="m.title || m.label || m.query">{{ m.title || m.label || m.query || '—' }}</div></td>
                <td class="muted"><span class="ellip mono text-xs" style="display:block" :title="m.workspace_id">{{ m.workspace_id || '—' }}</span></td>
                <td class="num mono">{{ num(m.gap_score) }}</td>
                <td class="muted text-sm">{{ m.detected_at ? fmtAgo(m.detected_at) : '—' }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Прогоны -->
        <div v-else-if="activeTab==='runs'" class="table-wrap">
          <table class="tbl tbl--fixed">
            <colgroup><col style="width:22%"><col style="width:15%"><col style="width:18%"><col style="width:45%"></colgroup>
            <thead><tr><th>Начат</th><th>Статус</th><th>Workspace</th><th>Сводка</th></tr></thead>
            <tbody>
              <tr v-for="(r, i) in currentRows" :key="r.id ?? i" style="cursor:pointer" @click="openDetail(r)">
                <td class="muted text-sm">{{ r.started_at ? fmtDate(r.started_at) : '—' }}</td>
                <td><UiBadge :variant="statusVariant(r.status)" :text="r.status || '—'" :dot="false"/></td>
                <td class="muted"><span class="ellip mono text-xs" style="display:block" :title="r.workspace_id">{{ r.workspace_id || 'все' }}</span></td>
                <td class="muted"><span class="ellip text-xs mono" style="display:block" :title="JSON.stringify(r.summary || {})">{{ JSON.stringify(r.summary || {}) }}</span></td>
              </tr>
            </tbody>
          </table>
        </div>

      </StateBlock>
    </div></div>

    <!-- Детали строки -->
    <UiModal v-if="detail" :title="detailTitle(detail)" size="lg" @close="closeDetail">
      <div class="dl">
        <template v-for="(v, k) in detail" :key="k">
          <dt class="mono">{{ k }}</dt>
          <dd>
            <span v-if="v === null || v === undefined" class="faint">—</span>
            <span v-else-if="typeof v === 'object'"><JsonView :data="v"/></span>
            <span v-else class="ellip" style="display:block" :title="String(v)">{{ v }}</span>
          </dd>
        </template>
      </div>

      <template v-if="timeline">
        <h4 class="mt-4 mb-3">Timeline</h4>
        <div v-if="seriesRows().length" class="table-wrap">
          <table class="tbl">
            <thead><tr><th>Момент</th><th class="num">Значение</th></tr></thead>
            <tbody>
              <tr v-for="(pt, i) in seriesRows()" :key="i">
                <td class="muted text-sm">{{ pt.t || pt.date || pt.detected_at || pt.at || '—' }}</td>
                <td class="num mono">{{ num(pt.value ?? pt.score ?? pt.signal_score ?? pt.count) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-else class="muted text-sm">Нет точек в ряду.</div>
      </template>
      <div v-else-if="timelineErr" class="field__error mt-2">{{ timelineErr }}</div>
    </UiModal>
  </div>
  `,
};
