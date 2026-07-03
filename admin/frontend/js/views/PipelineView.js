import { api } from '../api.js';
import { notify, confirmDialog } from '../ui.js';
import { fmtAgo, fmtDate, fmtDuration, fmtNum, statusVariant, truncate } from '../format.js';

// Ручные джобы: маршрут, подпись и текст подтверждения (все дорогие/CPU-bound).
const JOBS = [
  { key: 'signal', route: '/api/pipeline/run-signal-analysis', label: 'Signal analysis',
    desc: 'Анализ слабых сигналов (CPU-bound, долго)', ws: true },
  { key: 'missing', route: '/api/pipeline/run-missing-signals', label: 'Missing signals',
    desc: 'Поиск отсутствующих сигналов через SearXNG', ws: true },
  { key: 'clusters', route: '/api/pipeline/run-semantic-clusters', label: 'Semantic clusters',
    desc: 'Пересборка семантических кластеров', ws: true, wsQuery: true },
  { key: 'scores', route: '/api/pipeline/refresh-source-scores', label: 'Refresh source scores',
    desc: 'Пересчёт скоров источников', ws: true },
];

// Статусы индексации для мини-статистики — как на дашборде.
const STATUS_LABELS = {
  done: 'Готово', indexed: 'Проиндексировано', processing: 'В обработке',
  pending: 'В очереди', queued: 'В очереди', skipped: 'Пропущено',
  error: 'Ошибка', failed: 'Ошибка', dropped: 'Отброшено', empty: 'Пусто',
};

export default {
  name: 'PipelineView',
  data() {
    return {
      loading: true, error: null,
      workspace: this.$route.query.ws || '',
      scheduler: null,
      jobs: [],
      stats: {},
      recent: [],
      streams: null,
      onlyRunning: false,
      urgentDryRun: true,
      running: {},          // key джобы → busy
      jobDetail: null,      // выбранная manual-джоба для модалки результата
      streamsOpen: false,   // модалка полного снимка стримов
    };
  },
  computed: {
    jobDefs() { return JOBS; },
    statCards() {
      return Object.entries(this.stats || {}).map(([status, v]) => ({
        status,
        label: STATUS_LABELS[status] || status,
        count: (v && v.count) != null ? v.count : 0,
        variant: statusVariant(status),
      }));
    },
    schedulerJobs() {
      return (this.scheduler && Array.isArray(this.scheduler.jobs)) ? this.scheduler.jobs : [];
    },
    streamRows() {
      return (this.streams && Array.isArray(this.streams.streams)) ? this.streams.streams : [];
    },
    anyRunning() {
      return Object.values(this.running).some(Boolean);
    },
  },
  watch: {
    workspace(v) {
      this.$router.replace({ query: { ...this.$route.query, ws: v || undefined } });
      this.load();
    },
    onlyRunning() { this.loadJobs(); },
  },
  mounted() { this.load(); },
  methods: {
    fmtAgo, fmtDate, fmtDuration, fmtNum, statusVariant, truncate,
    jobDuration(j) {
      if (!j.started_at || !j.finished_at) return null;
      const a = new Date(j.started_at).getTime(); const b = new Date(j.finished_at).getTime();
      if (isNaN(a) || isNaN(b)) return null;
      return (b - a) / 1000;
    },
    async load() {
      this.loading = true; this.error = null;
      const ws = this.workspace || undefined;
      try {
        const [scheduler, stats, jobs, streams] = await Promise.all([
          api.get('/api/pipeline/scheduler').catch(() => null),
          api.get('/api/pipeline/stats', { workspace_id: ws }).catch(() => ({})),
          api.get('/api/pipeline/jobs/manual', {
            limit: 20, workspace_id: ws, only_running: this.onlyRunning || undefined,
          }).catch(() => ({})),
          api.get('/api/pipeline/streams').catch(() => null),
        ]);
        this.scheduler = scheduler;
        this.stats = (stats && stats.stats) || {};
        this.recent = (stats && Array.isArray(stats.recent)) ? stats.recent : [];
        this.jobs = (jobs && Array.isArray(jobs.jobs)) ? jobs.jobs : [];
        this.streams = streams;
      } catch (e) { this.error = e; }
      finally { this.loading = false; }
    },
    async loadJobs() {
      try {
        const data = await api.get('/api/pipeline/jobs/manual', {
          limit: 20, workspace_id: this.workspace || undefined, only_running: this.onlyRunning || undefined,
        });
        this.jobs = (data && Array.isArray(data.jobs)) ? data.jobs : [];
      } catch (e) { notify.error('Не удалось обновить джобы', e.detail); }
    },
    // ---- Запуск дорогих джобов ----
    async runJob(def) {
      const ok = await confirmDialog({
        title: `Запустить «${def.label}»?`,
        message: `${def.desc}. Джоба ресурсоёмкая${this.workspace ? ` (workspace: ${this.workspace})` : ' (все workspace)'} — запускать?`,
        confirmText: 'Запустить',
      });
      if (!ok) return;
      this.running = { ...this.running, [def.key]: true };
      try {
        const params = def.wsQuery ? { workspace_id: this.workspace || undefined } : undefined;
        const body = def.wsQuery ? undefined : { workspace_id: this.workspace || null };
        await api.post(def.route, body, params);
        notify.success('Джоба поставлена в очередь', def.label);
        setTimeout(() => this.loadJobs(), 1500);
      } catch (e) { notify.error(`«${def.label}» не запущена`, e.detail); }
      finally { this.running = { ...this.running, [def.key]: false }; }
    },
    async runUrgent() {
      const ok = await confirmDialog({
        title: 'Urgent trend alerts?',
        message: this.urgentDryRun
          ? 'Пробный прогон (dry-run): алерты не рассылаются, только считаются.'
          : 'БОЕВОЙ прогон: срочные трендовые алерты будут разосланы.',
        confirmText: this.urgentDryRun ? 'Прогнать' : 'Разослать',
        danger: !this.urgentDryRun,
      });
      if (!ok) return;
      this.running = { ...this.running, urgent: true };
      try {
        await api.post('/api/pipeline/run-urgent-trend-alerts', undefined, { dry_run: this.urgentDryRun || undefined });
        notify.success('Urgent trend alerts запущены', this.urgentDryRun ? 'dry-run' : 'боевой');
        setTimeout(() => this.loadJobs(), 1500);
      } catch (e) { notify.error('Urgent trend alerts не запущены', e.detail); }
      finally { this.running = { ...this.running, urgent: false }; }
    },
    // ---- Переиндексация поста ----
    async reprocess(post) {
      const ok = await confirmDialog({
        title: 'Переиндексировать пост?',
        message: `Пост ${truncate(post.id, 16)} будет переобработан пайплайном заново.`,
        confirmText: 'Переиндексировать',
      });
      if (!ok) return;
      try {
        await api.post(`/api/pipeline/reprocess/${encodeURIComponent(post.id)}`);
        notify.success('Пост отправлен на переиндексацию', truncate(post.id, 16));
        setTimeout(() => this.load(), 1500);
      } catch (e) { notify.error('Не удалось переиндексировать', e.detail); }
    },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Пайплайн</h1>
        <p>Мониторинг обработки, планировщик и ручные джобы</p>
      </div>
      <div class="page-head__actions">
        <router-link to="/" class="btn btn--outline btn--sm">← Обзор</router-link>
        <button class="btn btn--outline" @click="load" :disabled="loading">↻ Обновить</button>
      </div>
    </div>

    <div class="toolbar">
      <div class="toolbar__group"><label>Workspace</label><WorkspaceSelect v-model="workspace"/></div>
    </div>

    <StateBlock :loading="loading" :error="error" :empty="false" @retry="load">

      <div class="grid grid--2" style="margin-bottom:var(--sp-4)">
        <!-- Scheduler -->
        <div class="card">
          <div class="card__head">
            <h2>Планировщик</h2>
            <UiBadge v-if="scheduler" :variant="scheduler.running ? 'success' : 'neutral'"
                     :text="scheduler.running ? 'работает' : 'остановлен'"/>
          </div>
          <div class="card__body">
            <div v-if="!scheduler" class="muted text-sm">Статус недоступен</div>
            <template v-else>
              <div class="dl" style="margin-bottom:var(--sp-3)">
                <dt>Включён</dt><dd>{{ scheduler.enabled ? 'да' : 'нет' }}</dd>
                <dt>Запущен</dt><dd>{{ scheduler.running ? 'да' : 'нет' }}</dd>
                <dt>Таймзона</dt><dd class="mono">{{ scheduler.timezone || 'UTC' }}</dd>
                <dt>Заданий</dt><dd>{{ schedulerJobs.length }}</dd>
              </div>
              <div v-if="schedulerJobs.length" class="table-wrap">
                <table class="tbl tbl--fixed" style="min-width:0">
                  <colgroup><col style="width:55%"><col style="width:45%"></colgroup>
                  <thead><tr><th>Задание</th><th>Следующий запуск</th></tr></thead>
                  <tbody>
                    <tr v-for="j in schedulerJobs" :key="j.id">
                      <td><span class="ellip mono text-sm" :title="j.id">{{ j.id }}</span></td>
                      <td class="muted text-sm">{{ j.next_run_time ? fmtDate(j.next_run_time) : '—' }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </template>
          </div>
        </div>

        <!-- Ручные джобы -->
        <div class="card">
          <div class="card__head"><h2>Ручные джобы</h2><span class="sub">ресурсоёмкие — с подтверждением</span></div>
          <div class="card__body">
            <div class="stack">
              <div v-for="d in jobDefs" :key="d.key" class="row" style="justify-content:space-between;gap:var(--sp-3)">
                <div style="min-width:0">
                  <div class="text-sm" style="font-weight:600">{{ d.label }}</div>
                  <div class="text-xs faint ellip" :title="d.desc">{{ d.desc }}</div>
                </div>
                <button class="btn btn--outline btn--sm" :disabled="anyRunning" @click="runJob(d)">
                  <span v-if="running[d.key]" class="spin"></span> Запустить
                </button>
              </div>
              <!-- Urgent trend alerts (с dry_run) -->
              <div class="row" style="justify-content:space-between;gap:var(--sp-3);align-items:flex-start">
                <div style="min-width:0">
                  <div class="text-sm" style="font-weight:600">Urgent trend alerts</div>
                  <label class="checkbox text-xs faint"><input type="checkbox" v-model="urgentDryRun"> dry-run (не рассылать)</label>
                </div>
                <button class="btn btn--sm" :class="urgentDryRun ? 'btn--outline' : 'btn--danger'"
                        :disabled="anyRunning" @click="runUrgent">
                  <span v-if="running.urgent" class="spin"></span> {{ urgentDryRun ? 'Прогнать' : 'Разослать' }}
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Мини-статистика статусов -->
      <div v-if="statCards.length" class="grid grid--stats" style="margin-bottom:var(--sp-4)">
        <div v-for="c in statCards" :key="c.status" class="stat">
          <div class="stat__label"><UiBadge :variant="c.variant" :text="c.label"/></div>
          <div class="stat__value">{{ fmtNum(c.count) }}</div>
        </div>
      </div>

      <!-- Последние manual-джобы -->
      <div class="card">
        <div class="card__head">
          <h2>История ручных джобов</h2>
          <span class="spacer"></span>
          <label class="checkbox text-sm"><input type="checkbox" v-model="onlyRunning"> только активные</label>
        </div>
        <div class="card__body--flush">
          <StateBlock :empty="!jobs.length" empty-text="Ручных джобов пока нет">
            <div class="table-wrap">
              <table class="tbl tbl--fixed">
                <colgroup>
                  <col style="width:26%"><col style="width:16%"><col style="width:14%">
                  <col style="width:18%"><col style="width:12%"><col style="width:14%">
                </colgroup>
                <thead><tr>
                  <th>Джоба</th><th>Workspace</th><th>Статус</th>
                  <th>Начало</th><th class="num">Длительность</th><th></th>
                </tr></thead>
                <tbody>
                  <tr v-for="j in jobs" :key="j.id">
                    <td><span class="ellip mono text-sm" :title="j.job_name">{{ j.job_name || '—' }}</span></td>
                    <td class="muted"><span class="ellip" style="display:block" :title="j.workspace_id">{{ j.workspace_id || 'все' }}</span></td>
                    <td><UiBadge :variant="statusVariant(j.status)" :text="j.status || '—'"/></td>
                    <td class="muted text-sm">{{ j.started_at ? fmtAgo(j.started_at) : '—' }}</td>
                    <td class="num mono">{{ fmtDuration(jobDuration(j)) }}</td>
                    <td class="actions">
                      <button class="btn btn--sm btn--ghost" @click="jobDetail = j" title="Результат">Результат</button>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </StateBlock>
        </div>
      </div>

      <!-- Последние посты + reprocess -->
      <div class="card">
        <div class="card__head"><h2>Последние посты</h2></div>
        <div class="card__body--flush">
          <StateBlock :empty="!recent.length" empty-text="Постов пока нет">
            <div class="table-wrap">
              <table class="tbl tbl--fixed">
                <colgroup>
                  <col style="width:16%"><col style="width:36%"><col style="width:15%">
                  <col style="width:11%"><col style="width:14%"><col style="width:8%">
                </colgroup>
                <thead><tr>
                  <th>Источник</th><th>Превью</th><th>Категория</th>
                  <th class="num">Релевантность</th><th>Статус</th><th></th>
                </tr></thead>
                <tbody>
                  <tr v-for="p in recent" :key="p.id">
                    <td><span class="ellip mono text-sm" :title="p.source_id">{{ p.source_id || '—' }}</span></td>
                    <td class="muted"><span class="ellip" style="display:block" :title="p.preview">{{ p.preview || '—' }}</span></td>
                    <td><span class="ellip" style="display:block" :title="p.category">{{ p.category || '—' }}</span></td>
                    <td class="num mono">{{ p.relevance_score != null ? Number(p.relevance_score).toFixed(2) : '—' }}</td>
                    <td><UiBadge v-if="p.embedding_status" :variant="statusVariant(p.embedding_status)" :text="p.embedding_status" :dot="false"/></td>
                    <td class="actions">
                      <button class="btn btn--sm btn--outline" @click="reprocess(p)" title="Переиндексировать">↺</button>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </StateBlock>
        </div>
      </div>

      <!-- Redis Streams snapshot -->
      <div class="card">
        <div class="card__head">
          <h2>Redis Streams</h2>
          <span class="spacer"></span>
          <button v-if="streams" class="btn btn--sm btn--ghost" @click="streamsOpen = true">Полный снимок</button>
        </div>
        <div class="card__body--flush">
          <StateBlock :empty="!streamRows.length" empty-text="Снимок стримов недоступен">
            <div class="table-wrap">
              <table class="tbl tbl--fixed">
                <colgroup>
                  <col style="width:32%"><col style="width:22%"><col style="width:15%">
                  <col style="width:15%"><col style="width:16%">
                </colgroup>
                <thead><tr>
                  <th>Stream</th><th>Group</th><th class="num">Lag</th>
                  <th class="num">Pending</th><th class="num">Oldest pending</th>
                </tr></thead>
                <tbody>
                  <tr v-for="(s, i) in streamRows" :key="i">
                    <td><span class="ellip mono text-sm" :title="s.stream">{{ s.stream || '—' }}</span></td>
                    <td class="muted"><span class="ellip mono text-sm" style="display:block" :title="s.group">{{ s.group || '—' }}</span></td>
                    <td class="num mono" :class="Number(s.lag) ? '' : 'faint'">{{ fmtNum(s.lag || 0) }}</td>
                    <td class="num mono" :class="Number(s.pending) ? '' : 'faint'">{{ fmtNum(s.pending || 0) }}</td>
                    <td class="num mono" :class="Number(s.oldest_pending_age_seconds) ? '' : 'faint'">{{ fmtDuration(s.oldest_pending_age_seconds || 0) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </StateBlock>
        </div>
      </div>

    </StateBlock>

    <!-- Модалка: результат manual-джобы -->
    <UiModal v-if="jobDetail" :title="'Джоба: ' + (jobDetail.job_name || jobDetail.id)" size="lg" @close="jobDetail = null">
      <div class="dl" style="margin-bottom:var(--sp-3)">
        <dt>Статус</dt><dd><UiBadge :variant="statusVariant(jobDetail.status)" :text="jobDetail.status || '—'"/></dd>
        <dt>Workspace</dt><dd>{{ jobDetail.workspace_id || 'все' }}</dd>
        <dt>Начало</dt><dd>{{ jobDetail.started_at ? fmtDate(jobDetail.started_at) : '—' }}</dd>
        <dt>Завершение</dt><dd>{{ jobDetail.finished_at ? fmtDate(jobDetail.finished_at) : '—' }}</dd>
        <dt>Длительность</dt><dd>{{ fmtDuration(jobDuration(jobDetail)) }}</dd>
      </div>
      <h4 class="mb-3">Результат</h4>
      <JsonView :data="jobDetail.result != null ? jobDetail.result : jobDetail"/>
    </UiModal>

    <!-- Модалка: полный снимок стримов -->
    <UiModal v-if="streamsOpen" title="Redis Streams — снимок" size="lg" @close="streamsOpen = false">
      <JsonView :data="streams"/>
    </UiModal>
  </div>
  `,
};
