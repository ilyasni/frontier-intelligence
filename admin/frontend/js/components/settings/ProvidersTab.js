import { api } from '../../api.js';
import SettingCard from './SettingCard.js';
import KvGrid from './KvGrid.js';
import { fmtCompact, fmtPct01, fmtTs, toneForStatus, uniqueStrings, relDuration } from './helpers.js';

// Вкладка «Провайдеры и FinOps»: каждая секция грузится независимо (свой loading/error).
export default {
  name: 'ProvidersTab',
  components: { SettingCard, KvGrid },
  data() {
    return {
      s: {}, // { key: {data, loading, error} } — состояние по секциям
    };
  },
  computed: {
    providerState() { return this.get('providerState'); },
    budgetState() { return this.get('budgetState'); },
    capability() { return this.get('capability'); },
    circuits() { return this.get('circuits'); },
    events() { return this.get('events'); },
    wormLimits() { return this.get('wormLimits'); },
    wormModels() { return this.get('wormModels'); },
    orKey() { return this.get('orKey'); },
    orCatalog() { return this.get('orCatalog'); },
    orHealth() { return this.get('orHealth'); },
    orRuntime() { return this.get('orRuntime'); },
    balance() { return this.get('balance'); },
    weekly() { return this.get('weekly'); },
  },
  mounted() { this.loadAll(); },
  methods: {
    fmtCompact, fmtPct01, fmtTs, toneForStatus, relDuration,
    get(key) { return this.s[key] || { data: null, loading: true, error: null }; },
    // Универсальный загрузчик секции с изоляцией ошибок.
    async section(key, path) {
      this.s[key] = { data: this.s[key] && this.s[key].data, loading: true, error: null };
      try {
        const data = await api.get(path);
        this.s[key] = { data, loading: false, error: null };
      } catch (e) {
        this.s[key] = { data: null, loading: false, error: e };
      }
    },
    loadAll() {
      this.section('providerState', '/api/settings/provider-state');
      this.section('budgetState', '/api/settings/budget-state');
      this.section('capability', '/api/settings/capability-matrix');
      this.section('circuits', '/api/settings/circuits');
      this.section('events', '/api/settings/routing-events?limit=25');
      this.section('wormLimits', '/api/settings/providers/wormsoft/limits');
      this.section('wormModels', '/api/settings/providers/wormsoft/models');
      this.section('orKey', '/api/settings/providers/openrouter/key');
      this.section('orCatalog', '/api/settings/providers/openrouter/catalog');
      this.section('orHealth', '/api/settings/providers/openrouter/health');
      this.section('orRuntime', '/api/settings/providers/openrouter/runtime');
      this.section('balance', '/api/settings/gigachat-balance');
      this.section('weekly', '/api/settings/gigachat-weekly-report');
    },
    // ---- derived helpers ----
    providerRows() { return (this.providerState.data && this.providerState.data.providers) || []; },
    nonReady() { return this.providerRows().filter((p) => !p.ready).length; },
    budgetRows() { return (this.budgetState.data && this.budgetState.data.budgets) || []; },
    costRows() { return ((this.budgetState.data && this.budgetState.data.costs) || []).filter((c) => String(c.scope || '') === 'cost_provider'); },
    budgetSummary() { return (this.budgetState.data && this.budgetState.data.summary) || {}; },
    capRows() { return (this.capability.data && this.capability.data.rows) || []; },
    circuitRows() { return (this.circuits.data && this.circuits.data.circuits) || []; },
    wormsoftGuard() { return (this.circuits.data && this.circuits.data.wormsoft_guard) || {}; },
    eventRows() { return (this.events.data && this.events.data.events) || []; },
    balanceItems() { return (this.balance.data && this.balance.data.balance) || []; },
    balanceThreshold() { return Number((this.balance.data && this.balance.data.alert_threshold) || 100000); },
    balanceCells() {
      return this.balanceItems().map((it) => {
        const low = Number(it.value || 0) < this.balanceThreshold();
        return {
          label: String(it.usage || 'usage'),
          value: fmtCompact(it.value),
          meta: low ? `Ниже порога ${fmtCompact(this.balanceThreshold())}` : 'Выше порога алерта',
          badge: low ? 'warn' : 'success',
        };
      });
    },
    budgetCells() {
      const sum = this.budgetSummary();
      return [
        { label: 'Провайдеров', value: fmtCompact(sum.provider_count || this.costRows().length), meta: 'В runtime finops-итогах' },
        { label: 'Запросов (runtime)', value: fmtCompact(sum.request_count || 0), meta: 'Primary + shadow' },
        { label: 'Actual cost', value: fmtCompact(sum.actual_cost_total || 0), meta: 'Накопленная фактическая стоимость' },
        { label: 'Cost drift', value: fmtCompact(sum.cost_drift_total || 0), meta: 'Фактическая − оценочная' },
      ];
    },
    circuitCells() {
      const g = this.wormsoftGuard();
      const c = this.circuitRows();
      return [
        { label: 'Wormsoft guard', value: g.quarantined ? 'карантин' : 'чисто', meta: g.quarantine_until ? relDuration(g.quarantine_until) : 'Нет карантина', badge: g.quarantined ? 'danger' : 'success' },
        { label: 'Открытых circuits', value: String(c.length), meta: 'Provider + model', badge: c.length ? 'warn' : 'success' },
        { label: 'Provider circuits', value: String(c.filter((i) => i.level === 'provider').length), meta: 'Пробои на уровне провайдера' },
        { label: 'Model circuits', value: String(c.filter((i) => i.level === 'model').length), meta: 'Пробои на уровне модели' },
      ];
    },
    orKeyCells() {
      const d = this.orKey.data || {};
      const cells = [
        { label: 'Статус', value: String(d.status || 'unavailable'), meta: 'Наличие и доступность ключа', badge: toneForStatus(d.status) },
        { label: 'Тир', value: d.is_free_tier ? 'Free' : 'Paid', meta: d.is_free_tier ? 'Только free-модели (50 RPD)' : '1000 RPD на free-моделях' },
        { label: 'Free RPD', value: String(d.free_model_daily_limit != null ? d.free_model_daily_limit : (d.is_free_tier ? 50 : 1000)), meta: 'Дневной лимит запросов' },
        { label: 'Free RPM', value: String(d.free_model_rpm_limit != null ? d.free_model_rpm_limit : 20), meta: 'Лимит в минуту' },
        { label: 'Расход сегодня', value: `$${Number(d.usage_daily || 0).toFixed(4)}`, meta: 'Платные токены за день' },
        { label: 'За неделю', value: `$${Number(d.usage_weekly || 0).toFixed(4)}`, meta: 'Накопленный платный расход' },
        { label: 'За месяц', value: `$${Number(d.usage_monthly || 0).toFixed(4)}`, meta: 'Накопленный платный расход' },
      ];
      if (d.limit != null) cells.push({ label: 'Остаток кредита', value: `$${Number(d.limit_remaining || 0).toFixed(4)}`, meta: 'Предоплаченный баланс' });
      return cells;
    },
    weeklyCells() {
      const r = this.weekly.data || {};
      const sum = (arr) => (arr || []).reduce((s, x) => s + Number(x.value || 0), 0);
      const reqs = r.requests || [];
      const totalReq = sum(reqs);
      const errReq = reqs.filter((x) => x.status !== 'ok').reduce((s, x) => s + Math.max(0, Number(x.value || 0)), 0);
      const done = (r.pipeline_status || []).find((x) => x.status === 'done');
      return [
        { label: 'Запросов / 7д', value: fmtCompact(Math.round(totalReq)), meta: 'Все задачи и статусы' },
        { label: 'Billable токены / 7д', value: fmtCompact(Math.round(sum(r.billable_tokens))), meta: 'По task/model' },
        { label: 'Эскалаций / 7д', value: fmtCompact(Math.round(sum(r.escalations))), meta: 'Переходы между моделями' },
        { label: '429 / 7д', value: fmtCompact(Math.round(sum(r.rate_limits))), meta: 'Rate-limit давление' },
        { label: 'Done share', value: done && done.share != null ? fmtPct01(done.share) : '—', meta: 'Доля done среди done/dropped/error' },
        { label: 'Ошибок LLM', value: totalReq > 0 ? fmtPct01(errReq / totalReq) : '—', meta: 'Доля non-ok запросов', badge: errReq ? 'warn' : 'success' },
      ];
    },
    wormLimitCells() {
      const d = this.wormLimits.data || {};
      const plans = Array.isArray(d.plans) ? d.plans : [];
      return [
        { label: 'Ключ', value: d.key_present ? 'есть' : 'нет', meta: 'API-ключ в .env на сервере', badge: d.key_present ? 'success' : 'danger' },
        { label: 'Статус', value: String(d.status || 'unavailable'), meta: d.stale ? 'Показан кэш (свежий fetch не удался)' : 'Живой fetch', badge: toneForStatus(d.status) },
        { label: 'Планов', value: String(d.plan_count || plans.length || 0), meta: 'Опубликованные окна подписки' },
        { label: 'Моделей с ценой', value: String(d.pricing_model_count || Object.keys(d.pricing || {}).length || 0), meta: 'Кредиты за 1M токенов' },
      ];
    },
    orRuntimeCells() {
      const d = this.orRuntime.data || {};
      return [
        { label: 'Активная модель', value: String(d.active_model || d.model || '—'), meta: 'Текущий выбор OpenRouter-пикера' },
        { label: 'Кандидатов', value: String((d.candidates || d.models || []).length), meta: 'Доступные модели в ротации' },
        { label: 'Обновлено', value: fmtTs(d.updated_at || d.refreshed_at), meta: 'Последнее обновление рантайма' },
      ];
    },
  },
  template: `
  <div class="stack" style="gap:var(--sp-4)">
    <div class="page-head" style="margin-bottom:0">
      <div class="page-head__text"><p class="muted text-sm">Единый обзор здоровья провайдеров, бюджетов и стоимости. Секции грузятся независимо.</p></div>
      <div class="page-head__actions"><button class="btn btn--outline btn--sm" @click="loadAll">↻ Обновить всё</button></div>
    </div>

    <!-- Provider state -->
    <SettingCard title="Состояние провайдеров"
      desc="Здоровье, readiness и давление квот по всем провайдерам."
      :badge-text="providerRows().length ? (nonReady() ? nonReady() + ' degraded' : 'Live') : ''"
      :badge-variant="providerRows().length ? (nonReady() ? 'warn' : 'success') : 'neutral'"
      :loading="providerState.loading" :error="providerState.error"
      :empty="!providerRows().length" empty-text="Снимков нет"
      @retry="section('providerState', '/api/settings/provider-state')">
      <div class="table-wrap">
        <table class="tbl">
          <thead><tr><th>Провайдер</th><th>Ready</th><th>Readiness</th><th>Health</th><th>Quota</th><th class="num">Budgets</th></tr></thead>
          <tbody>
            <tr v-for="(p, i) in providerRows()" :key="i">
              <td class="mono">{{ p.provider || '—' }}</td>
              <td><UiBadge :variant="p.ready ? 'success' : 'danger'" :text="p.ready ? 'да' : 'нет'"/></td>
              <td><UiBadge :variant="toneForStatus(p.readiness_state)" :text="p.readiness_state || 'unknown'" :dot="false"/></td>
              <td class="muted">{{ p.health_status || '—' }}</td>
              <td class="muted">{{ p.quota_pressure || '—' }}</td>
              <td class="num">{{ (p.budgets || []).length }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <details class="mt-4"><summary class="muted text-sm" style="cursor:pointer">Raw provider-state</summary><div class="mt-2"><JsonView :data="providerState.data"/></div></details>
    </SettingCard>

    <!-- Budget state -->
    <SettingCard title="Бюджеты и стоимость (FinOps)"
      desc="Окна бюджетов/кредитов и агрегаты стоимости по провайдерам."
      :badge-text="budgetRows().length ? 'Отслеживается' : ''"
      :badge-variant="budgetRows().length ? 'success' : 'neutral'"
      :loading="budgetState.loading" :error="budgetState.error"
      @retry="section('budgetState', '/api/settings/budget-state')">
      <div class="stack" style="gap:var(--sp-4)">
        <KvGrid :items="budgetCells()"/>
        <div>
          <div class="muted text-sm mb-3">Окна бюджетов</div>
          <div class="table-wrap">
            <table class="tbl">
              <thead><tr><th>Провайдер</th><th>Scope</th><th>Окно</th><th>Остаток</th><th>Статус</th></tr></thead>
              <tbody>
                <tr v-for="(b, i) in budgetRows()" :key="i">
                  <td class="mono">{{ b.provider || '—' }}</td>
                  <td class="muted">{{ b.scope || '—' }}</td>
                  <td class="muted">{{ b.window_label || '—' }}</td>
                  <td class="mono">{{ b.remaining == null ? '—' : b.remaining }}</td>
                  <td><UiBadge :variant="toneForStatus(b.status)" :text="b.status || '—'" :dot="false"/></td>
                </tr>
                <tr v-if="!budgetRows().length"><td colspan="5" class="muted text-sm" style="text-align:center">Нормализованных окон нет</td></tr>
              </tbody>
            </table>
          </div>
        </div>
        <div v-if="costRows().length">
          <div class="muted text-sm mb-3">Агрегаты стоимости по провайдерам</div>
          <div class="table-wrap">
            <table class="tbl tbl--fixed" style="min-width:760px">
              <thead><tr><th>Провайдер</th><th class="num">Запросов</th><th class="num">Успех</th><th class="num">Ошибки</th><th class="num">Оценка</th><th class="num">Факт</th><th class="num">Drift</th></tr></thead>
              <tbody>
                <tr v-for="(c, i) in costRows()" :key="i">
                  <td class="mono">{{ c.provider || '—' }}</td>
                  <td class="num">{{ fmtCompact(c.request_count) }}</td>
                  <td class="num">{{ fmtCompact(c.success_count) }}</td>
                  <td class="num">{{ fmtCompact(c.error_count) }}</td>
                  <td class="num">{{ fmtCompact(c.estimated_cost_total) }}</td>
                  <td class="num">{{ fmtCompact(c.actual_cost_total) }}</td>
                  <td class="num">{{ fmtCompact(c.cost_drift_total) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
        <details><summary class="muted text-sm" style="cursor:pointer">Raw budget payload</summary><div class="mt-2"><JsonView :data="budgetState.data"/></div></details>
      </div>
    </SettingCard>

    <!-- Capability matrix -->
    <SettingCard title="Матрица возможностей"
      desc="Truth-таблица provider/model для text/vision/embeddings."
      :badge-text="capRows().length ? capRows().length + ' моделей' : ''"
      :badge-variant="capRows().length ? 'success' : 'neutral'"
      :loading="capability.loading" :error="capability.error"
      :empty="!capRows().length" empty-text="Строк нет"
      @retry="section('capability', '/api/settings/capability-matrix')">
      <div class="table-wrap">
        <table class="tbl tbl--fixed" style="min-width:900px">
          <thead><tr><th>Провайдер</th><th>Модель</th><th>Text</th><th>Vision</th><th>Embed</th><th class="num">Dim</th><th class="num">Context</th><th>Metric</th></tr></thead>
          <tbody>
            <tr v-for="(r, i) in capRows()" :key="i">
              <td class="mono">{{ r.provider || '—' }}</td>
              <td class="mono ellip" :title="r.model">{{ r.model || '—' }}</td>
              <td>{{ r.text_generation ? '✓' : '—' }}</td>
              <td>{{ r.vision_generation ? '✓' : '—' }}</td>
              <td>{{ r.embeddings ? '✓' : '—' }}</td>
              <td class="num">{{ r.dimension == null ? '—' : r.dimension }}</td>
              <td class="num">{{ r.context_tokens == null ? '—' : r.context_tokens }}</td>
              <td class="muted">{{ r.distance_metric || '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <details class="mt-4"><summary class="muted text-sm" style="cursor:pointer">Raw capability matrix</summary><div class="mt-2"><JsonView :data="capability.data"/></div></details>
    </SettingCard>

    <!-- Circuits -->
    <SettingCard title="Circuits"
      desc="Provider/model circuit breakers + общий Wormsoft guard."
      :badge-text="circuitRows().length || wormsoftGuard().quarantined ? 'Активны' : 'Чисто'"
      :badge-variant="circuitRows().length || wormsoftGuard().quarantined ? 'warn' : 'success'"
      :loading="circuits.loading" :error="circuits.error"
      @retry="section('circuits', '/api/settings/circuits')">
      <div class="stack" style="gap:var(--sp-4)">
        <KvGrid :items="circuitCells()"/>
        <div class="table-wrap">
          <table class="tbl tbl--fixed" style="min-width:820px">
            <thead><tr><th>Level</th><th>Провайдер</th><th>Модель</th><th>State</th><th>Reason</th><th>До</th><th class="num">Отказов</th></tr></thead>
            <tbody>
              <tr v-for="(c, i) in circuitRows()" :key="i">
                <td class="muted">{{ c.level || '—' }}</td>
                <td class="mono">{{ c.provider || '—' }}</td>
                <td class="mono ellip" :title="c.model">{{ c.model || '—' }}</td>
                <td><UiBadge :variant="toneForStatus(c.state)" :text="c.state || '—'" :dot="false"/></td>
                <td class="muted ellip" :title="c.reason">{{ c.reason || '—' }}</td>
                <td class="muted">{{ c.opened_until ? relDuration(c.opened_until) : '—' }}</td>
                <td class="num">{{ fmtCompact(c.failure_count) }}</td>
              </tr>
              <tr v-if="!circuitRows().length"><td colspan="7" class="muted text-sm" style="text-align:center">Открытых circuits нет</td></tr>
            </tbody>
          </table>
        </div>
        <details><summary class="muted text-sm" style="cursor:pointer">Raw circuit payload</summary><div class="mt-2"><JsonView :data="circuits.data"/></div></details>
      </div>
    </SettingCard>

    <!-- Routing events -->
    <SettingCard title="Routing events"
      desc="Недавние routing_decision_made / routing_execution_finished из воркеров."
      :badge-text="eventRows().length ? eventRows().length + ' событий' : ''"
      :badge-variant="eventRows().length ? 'success' : 'neutral'"
      :loading="events.loading" :error="events.error"
      :empty="!eventRows().length" empty-text="Событий в Redis пока нет"
      @retry="section('events', '/api/settings/routing-events?limit=25')">
      <details><summary class="muted text-sm" style="cursor:pointer">Raw routing events ({{ eventRows().length }})</summary><div class="mt-2"><JsonView :data="events.data"/></div></details>
    </SettingCard>

    <div class="grid grid--2">
      <!-- Wormsoft limits -->
      <SettingCard title="Wormsoft: лимиты и цены"
        desc="Окна подписки Wormsoft и цены на маршрутизируемые модели."
        :badge-text="wormLimits.data ? (wormLimits.data.available ? 'Ready' : (wormLimits.data.stale ? 'Cached' : 'Unavailable')) : ''"
        :badge-variant="wormLimits.data ? toneForStatus(wormLimits.data.status) : 'neutral'"
        :loading="wormLimits.loading" :error="wormLimits.error"
        @retry="section('wormLimits', '/api/settings/providers/wormsoft/limits')">
        <div class="stack" style="gap:var(--sp-4)">
          <KvGrid :items="wormLimitCells()"/>
          <details><summary class="muted text-sm" style="cursor:pointer">Raw Wormsoft limits</summary><div class="mt-2"><JsonView :data="wormLimits.data"/></div></details>
        </div>
      </SettingCard>

      <!-- Wormsoft models -->
      <SettingCard title="Wormsoft: каталог моделей"
        :desc="'Каталог моделей провайдера Wormsoft.'"
        :badge-text="wormModels.data ? (wormModels.data.count != null ? wormModels.data.count + ' моделей' : String(wormModels.data.status || '')) : ''"
        :badge-variant="wormModels.data ? toneForStatus(wormModels.data.status) : 'neutral'"
        :loading="wormModels.loading" :error="wormModels.error"
        @retry="section('wormModels', '/api/settings/providers/wormsoft/models')">
        <details><summary class="muted text-sm" style="cursor:pointer">Raw каталог Wormsoft</summary><div class="mt-2"><JsonView :data="wormModels.data"/></div></details>
      </SettingCard>
    </div>

    <!-- OpenRouter key -->
    <SettingCard title="OpenRouter: ключ и лимиты"
      desc="Использование аккаунта и rate-лимиты free-моделей для fallback-провайдера."
      :badge-text="orKey.data ? (orKey.data.status === 'missing_api_key' ? 'Нет ключа' : (orKey.data.available ? (orKey.data.is_free_tier ? 'Free' : 'Paid') : 'Unavailable')) : ''"
      :badge-variant="orKey.data ? toneForStatus(orKey.data.status) : 'neutral'"
      :loading="orKey.loading" :error="orKey.error"
      @retry="section('orKey', '/api/settings/providers/openrouter/key')">
      <div class="stack" style="gap:var(--sp-4)">
        <KvGrid :items="orKeyCells()"/>
        <details><summary class="muted text-sm" style="cursor:pointer">Raw key payload</summary><div class="mt-2"><JsonView :data="orKey.data"/></div></details>
      </div>
    </SettingCard>

    <div class="grid grid--2">
      <!-- OpenRouter runtime -->
      <SettingCard title="OpenRouter: рантайм-пикер"
        desc="Активная модель и кандидаты в ротации OpenRouter."
        :loading="orRuntime.loading" :error="orRuntime.error"
        @retry="section('orRuntime', '/api/settings/providers/openrouter/runtime')">
        <div class="stack" style="gap:var(--sp-4)">
          <KvGrid :items="orRuntimeCells()"/>
          <details><summary class="muted text-sm" style="cursor:pointer">Raw runtime</summary><div class="mt-2"><JsonView :data="orRuntime.data"/></div></details>
        </div>
      </SettingCard>

      <!-- OpenRouter health -->
      <SettingCard title="OpenRouter: health"
        desc="Снимок доступности моделей OpenRouter (probe)."
        :badge-text="orHealth.data ? String(orHealth.data.status || '') : ''"
        :badge-variant="orHealth.data ? toneForStatus(orHealth.data.status) : 'neutral'"
        :loading="orHealth.loading" :error="orHealth.error"
        @retry="section('orHealth', '/api/settings/providers/openrouter/health')">
        <details><summary class="muted text-sm" style="cursor:pointer">Raw health</summary><div class="mt-2"><JsonView :data="orHealth.data"/></div></details>
      </SettingCard>
    </div>

    <!-- OpenRouter catalog -->
    <SettingCard title="OpenRouter: каталог"
      desc="Каталог моделей OpenRouter."
      :badge-text="orCatalog.data ? String(orCatalog.data.count != null ? orCatalog.data.count + ' моделей' : (orCatalog.data.status || '')) : ''"
      :badge-variant="orCatalog.data ? toneForStatus(orCatalog.data.status) : 'neutral'"
      :loading="orCatalog.loading" :error="orCatalog.error"
      @retry="section('orCatalog', '/api/settings/providers/openrouter/catalog')">
      <details><summary class="muted text-sm" style="cursor:pointer">Raw каталог OpenRouter</summary><div class="mt-2"><JsonView :data="orCatalog.data"/></div></details>
    </SettingCard>

    <!-- GigaChat balance -->
    <SettingCard title="GigaChat: баланс токенов"
      desc="Живой баланс пакетов по семействам моделей с порогом алерта."
      :badge-text="balance.data ? String(balance.data.status || '') : ''"
      :badge-variant="balance.data ? toneForStatus(balance.data.status) : 'neutral'"
      :loading="balance.loading" :error="balance.error"
      :empty="!balanceItems().length && !balance.loading" empty-text="Нет снимка баланса"
      @retry="section('balance', '/api/settings/gigachat-balance')">
      <div class="stack" style="gap:var(--sp-4)">
        <KvGrid :items="balanceCells()"/>
        <div class="muted text-xs">Порог алерта: {{ fmtCompact(balanceThreshold()) }} токенов.</div>
        <details><summary class="muted text-sm" style="cursor:pointer">Raw balance payload</summary><div class="mt-2"><JsonView :data="balance.data"/></div></details>
      </div>
    </SettingCard>

    <!-- GigaChat weekly -->
    <SettingCard title="GigaChat: недельный отчёт"
      desc="7-дневный снимок стоимости и качества из Prometheus и БД."
      :loading="weekly.loading" :error="weekly.error"
      @retry="section('weekly', '/api/settings/gigachat-weekly-report')">
      <div class="stack" style="gap:var(--sp-4)">
        <KvGrid :items="weeklyCells()"/>
        <details><summary class="muted text-sm" style="cursor:pointer">Raw weekly report</summary><div class="mt-2"><JsonView :data="weekly.data"/></div></details>
      </div>
    </SettingCard>

  </div>
  `,
};
