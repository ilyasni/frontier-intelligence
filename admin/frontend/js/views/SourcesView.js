import { api } from '../api.js';
import { notify, confirmDialog } from '../ui.js';
import { fmtAgo, fmtNum, statusVariant, truncate } from '../format.js';

const TYPE_LABELS = { telegram: 'Telegram', rss: 'RSS', web: 'Web', api: 'API', email: 'Email' };
const VISION_MODES = [
  { v: 'full', l: 'Полный (vision + OCR)' },
  { v: 'ocr_only', l: 'Только OCR' },
  { v: 'skip', l: 'Пропускать медиа' },
];

export default {
  name: 'SourcesView',
  data() {
    return {
      sources: [], catalog: null, loading: true, error: null, busy: false,
      filters: { workspace: this.$route.query.ws || '', q: '', type: '', status: '' },
      detail: null,
      addOpen: false, addForm: this.blankAdd(), addErr: '',
      visionOpen: false, visionSrc: null, visionForm: { mode: 'full', max_media_bytes: 9000000 },
      tgOpen: false, tgSrc: null, tgForm: { tg_channel: '' },
    };
  },
  computed: {
    typeOptions() {
      const cat = this.catalog && this.catalog.source_types;
      if (Array.isArray(cat) && cat.length) return cat.map((t) => (typeof t === 'string' ? t : t.value || t.id || t.type));
      return Object.keys(TYPE_LABELS);
    },
    filtered() {
      const q = this.filters.q.trim().toLowerCase();
      return this.sources.filter((s) => {
        if (this.filters.type && s.source_type !== this.filters.type) return false;
        if (this.filters.status === 'enabled' && !s.is_enabled) return false;
        if (this.filters.status === 'disabled' && s.is_enabled) return false;
        if (this.filters.status === 'error' && !(s.last_run_status === 'error' || s.last_error)) return false;
        if (q) {
          const hay = `${s.name || ''} ${s.id || ''} ${s.url || ''} ${s.tg_channel || ''}`.toLowerCase();
          if (!hay.includes(q)) return false;
        }
        return true;
      });
    },
    counts() {
      return {
        total: this.sources.length,
        enabled: this.sources.filter((s) => s.is_enabled).length,
        error: this.sources.filter((s) => s.last_run_status === 'error' || s.last_error).length,
      };
    },
  },
  watch: {
    'filters.workspace'(v) {
      this.$router.replace({ query: { ...this.$route.query, ws: v || undefined } });
      this.load();
    },
  },
  mounted() {
    this.load();
    api.get('/api/sources/catalog').then((c) => { this.catalog = c; }).catch(() => {});
  },
  methods: {
    fmtAgo, fmtNum, statusVariant, truncate,
    typeLabel(t) { return TYPE_LABELS[t] || t; },
    blankAdd() {
      return {
        id: '', workspace_id: this.$route.query.ws || '', source_type: 'rss', name: '',
        url: '', tg_channel: '', tg_account_idx: 0, schedule_cron: '', proxy_config: '', extra: '',
      };
    },
    async load() {
      this.loading = true; this.error = null;
      try {
        const data = await api.get('/api/sources', { workspace_id: this.filters.workspace || undefined });
        this.sources = Array.isArray(data) ? data : [];
      } catch (e) { this.error = e; }
      finally { this.loading = false; }
    },
    async toggle(s) {
      try {
        await api.patch(`/api/sources/${encodeURIComponent(s.id)}/toggle`);
        s.is_enabled = !s.is_enabled;
        notify.success(s.is_enabled ? 'Источник включён' : 'Источник выключен', s.name || s.id);
      } catch (e) { notify.error('Не удалось переключить', e.detail); }
    },
    async del(s) {
      const ok = await confirmDialog({
        title: 'Удалить источник?',
        message: `«${s.name || s.id}» будет удалён. Собранные посты сохранятся (source_id обнулится).`,
        confirmText: 'Удалить', danger: true,
      });
      if (!ok) return;
      try {
        await api.del(`/api/sources/${encodeURIComponent(s.id)}`);
        this.sources = this.sources.filter((x) => x.id !== s.id);
        if (this.detail && this.detail.id === s.id) this.detail = null;
        notify.success('Источник удалён', s.name || s.id);
      } catch (e) { notify.error('Не удалось удалить', e.detail); }
    },
    // ---- Add ----
    openAdd() { this.addForm = this.blankAdd(); this.addErr = ''; this.addOpen = true; },
    parseJsonField(v, label) {
      if (!v || !v.trim()) return {};
      try { const o = JSON.parse(v); if (o && typeof o === 'object') return o; throw 0; }
      catch { throw new Error(`${label}: невалидный JSON`); }
    },
    async submitAdd() {
      this.addErr = '';
      const f = this.addForm;
      if (!f.id.trim()) return (this.addErr = 'Укажи ID источника');
      if (!f.workspace_id.trim()) return (this.addErr = 'Выбери workspace');
      if (!f.name.trim()) return (this.addErr = 'Укажи имя');
      if (f.source_type === 'telegram' && !f.tg_channel.trim()) return (this.addErr = 'Укажи Telegram-канал');
      if (['rss', 'web', 'api'].includes(f.source_type) && !f.url.trim()) return (this.addErr = 'Укажи URL');
      let proxy_config, extra;
      try { proxy_config = this.parseJsonField(f.proxy_config, 'proxy_config'); extra = this.parseJsonField(f.extra, 'extra'); }
      catch (e) { return (this.addErr = e.message); }
      const body = {
        id: f.id.trim(), workspace_id: f.workspace_id, source_type: f.source_type, name: f.name.trim(),
        url: f.url.trim() || null, tg_channel: f.tg_channel.trim() || null,
        tg_account_idx: Number(f.tg_account_idx) || 0, schedule_cron: f.schedule_cron.trim() || null,
        proxy_config, extra,
      };
      this.busy = true;
      try {
        await api.post('/api/sources', body);
        notify.success('Источник сохранён', body.name);
        this.addOpen = false; this.load();
      } catch (e) { this.addErr = e.detail || 'Ошибка сохранения'; }
      finally { this.busy = false; }
    },
    // ---- Vision ----
    openVision(s) {
      this.visionSrc = s;
      const v = (s.extra && s.extra.vision) || {};
      this.visionForm = { mode: v.mode || 'full', max_media_bytes: v.max_media_bytes || 9000000 };
      this.visionOpen = true;
    },
    async submitVision() {
      this.busy = true;
      try {
        await api.patch(`/api/sources/${encodeURIComponent(this.visionSrc.id)}/vision`, {
          mode: this.visionForm.mode, max_media_bytes: Number(this.visionForm.max_media_bytes) || null,
        });
        notify.success('Vision-политика обновлена', this.visionSrc.name || this.visionSrc.id);
        this.visionOpen = false; this.load();
      } catch (e) { notify.error('Не удалось сохранить', e.detail); }
      finally { this.busy = false; }
    },
    // ---- Telegram handle ----
    openTg(s) { this.tgSrc = s; this.tgForm = { tg_channel: s.tg_channel || '' }; this.tgOpen = true; },
    async submitTg() {
      if (!this.tgForm.tg_channel.trim()) return;
      this.busy = true;
      try {
        await api.patch(`/api/sources/${encodeURIComponent(this.tgSrc.id)}/telegram-handle`, {
          tg_channel: this.tgForm.tg_channel.trim(),
        });
        notify.success('Канал обновлён', 'Курсор сброшен — сбор пойдёт заново');
        this.tgOpen = false; this.load();
      } catch (e) { notify.error('Не удалось сменить канал', e.detail); }
      finally { this.busy = false; }
    },
    // ---- Bulk ops ----
    async bootstrap() {
      const ok = await confirmDialog({
        title: 'Bootstrap из YAML?',
        message: 'Источники из config/sources.yml будут добавлены/обновлены (UPSERT, без удаления).',
        confirmText: 'Запустить',
      });
      if (!ok) return;
      this.busy = true;
      try {
        await api.post('/api/sources/bootstrap', undefined, { workspace_id: this.filters.workspace || undefined });
        notify.success('Bootstrap выполнен'); this.load();
      } catch (e) { notify.error('Bootstrap не удался', e.detail); }
      finally { this.busy = false; }
    },
    async refreshScores() {
      this.busy = true;
      try {
        await api.post('/api/pipeline/refresh-source-scores', { workspace_id: this.filters.workspace || undefined });
        notify.success('Пересчёт скоров запущен'); setTimeout(() => this.load(), 1200);
      } catch (e) { notify.error('Не удалось запустить', e.detail); }
      finally { this.busy = false; }
    },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Источники</h1>
        <p>{{ counts.total }} всего · {{ counts.enabled }} активны · <span :class="counts.error ? '' : 'faint'">{{ counts.error }} с ошибками</span></p>
      </div>
      <div class="page-head__actions">
        <button class="btn btn--outline btn--sm" :disabled="busy" @click="refreshScores">Обновить скоры</button>
        <button class="btn btn--outline btn--sm" :disabled="busy" @click="bootstrap">Bootstrap YAML</button>
        <button class="btn btn--primary" @click="openAdd">+ Добавить</button>
      </div>
    </div>

    <div class="toolbar">
      <div class="toolbar__group"><label>Workspace</label><WorkspaceSelect v-model="filters.workspace"/></div>
      <div class="toolbar__group"><label>Тип</label>
        <select class="select" v-model="filters.type">
          <option value="">Все типы</option>
          <option v-for="t in typeOptions" :key="t" :value="t">{{ typeLabel(t) }}</option>
        </select>
      </div>
      <div class="toolbar__group"><label>Статус</label>
        <select class="select" v-model="filters.status">
          <option value="">Любой</option>
          <option value="enabled">Активные</option>
          <option value="disabled">Выключенные</option>
          <option value="error">С ошибками</option>
        </select>
      </div>
      <div class="toolbar__group" style="flex:1;min-width:200px"><label>Поиск</label>
        <input class="input" v-model="filters.q" placeholder="имя, URL, канал, ID…">
      </div>
      <div class="toolbar__group"><label>&nbsp;</label><button class="btn btn--outline" @click="load" :disabled="loading">↻ Обновить</button></div>
    </div>

    <div class="card"><div class="card__body--flush">
      <StateBlock :loading="loading" :error="error" :empty="!filtered.length"
        :empty-text="sources.length ? 'Нет источников по фильтрам' : 'Источников пока нет'" @retry="load">
        <div class="table-wrap">
          <table class="tbl tbl--fixed">
            <colgroup>
              <col style="width:24%"><col style="width:26%"><col style="width:16%">
              <col style="width:9%"><col style="width:12%"><col style="width:13%">
            </colgroup>
            <thead><tr>
              <th>Источник</th><th>Цель</th><th>Статус</th>
              <th class="num">Успех / Ошибки</th><th>Последний сбор</th><th></th>
            </tr></thead>
            <tbody>
              <tr v-for="s in filtered" :key="s.id">
                <td>
                  <div class="ellip" style="font-weight:600;cursor:pointer" @click="detail = s" :title="s.name || s.id">{{ s.name || s.id }}</div>
                  <div class="row" style="gap:6px;margin-top:4px">
                    <UiBadge variant="accent" :text="typeLabel(s.source_type)" :dot="false"/>
                    <span class="text-xs faint mono ellip">{{ s.id }}</span>
                  </div>
                </td>
                <td class="muted"><span class="ellip" style="display:block" :title="s.url || s.tg_channel">{{ s.url || s.tg_channel || '—' }}</span></td>
                <td>
                  <div class="row" style="gap:6px;flex-wrap:wrap">
                    <UiBadge :variant="s.is_enabled ? 'success' : 'neutral'" :text="s.is_enabled ? 'активен' : 'выкл'"/>
                    <UiBadge v-if="s.last_run_status" :variant="statusVariant(s.last_run_status)" :text="s.last_run_status" :dot="false"/>
                  </div>
                  <div v-if="s.last_error" class="ellip text-xs" style="color:#fca5a5;margin-top:4px" :title="s.last_error">{{ s.last_error }}</div>
                </td>
                <td class="num mono">{{ fmtNum(s.recent_success_count) }} / <span :class="s.recent_error_count ? '' : 'faint'">{{ fmtNum(s.recent_error_count) }}</span></td>
                <td class="muted text-sm">{{ s.last_success_at ? fmtAgo(s.last_success_at) : '—' }}</td>
                <td class="actions">
                  <button class="btn btn--sm btn--outline" @click="toggle(s)">{{ s.is_enabled ? 'Выкл' : 'Вкл' }}</button>
                  <button class="btn btn--sm btn--ghost" @click="openVision(s)" title="Vision-политика">👁</button>
                  <button v-if="s.source_type==='telegram'" class="btn btn--sm btn--ghost" @click="openTg(s)" title="Сменить канал">🔗</button>
                  <button class="btn btn--sm btn--danger" @click="del(s)" title="Удалить">✕</button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </StateBlock>
    </div></div>

    <!-- Detail -->
    <UiModal v-if="detail" :title="detail.name || detail.id" size="lg" @close="detail = null">
      <div class="dl">
        <dt>ID</dt><dd class="mono">{{ detail.id }}</dd>
        <dt>Тип</dt><dd>{{ typeLabel(detail.source_type) }}</dd>
        <dt>Workspace</dt><dd>{{ detail.workspace_id }}</dd>
        <dt>Цель</dt><dd class="mono">{{ detail.url || detail.tg_channel || '—' }}</dd>
        <dt>Расписание</dt><dd class="mono">{{ detail.schedule_cron || '—' }}</dd>
        <dt>Свежесть</dt><dd>{{ detail.freshness_hours != null ? detail.freshness_hours + ' ч' : '—' }}</dd>
        <dt>Relevant ratio</dt><dd>{{ detail.relevant_ratio != null ? (detail.relevant_ratio*100).toFixed(0)+'%' : '—' }}</dd>
        <dt>Последняя ошибка</dt><dd>{{ detail.last_error || detail.error_text || '—' }}</dd>
      </div>
      <template v-if="detail.telegram_diagnostics">
        <h4 class="mt-4 mb-3">Telegram-диагностика</h4>
        <JsonView :data="detail.telegram_diagnostics"/>
      </template>
    </UiModal>

    <!-- Add -->
    <UiModal v-if="addOpen" title="Новый источник" size="lg" @close="addOpen = false">
      <div class="field-row">
        <div class="field"><label class="field__label">ID <span class="req">*</span></label><input class="input mono" v-model="addForm.id" placeholder="uniq_source_id"></div>
        <div class="field"><label class="field__label">Тип</label>
          <select class="select" v-model="addForm.source_type"><option v-for="t in typeOptions" :key="t" :value="t">{{ typeLabel(t) }}</option></select>
        </div>
      </div>
      <div class="field"><label class="field__label">Имя <span class="req">*</span></label><input class="input" v-model="addForm.name" placeholder="Читаемое название"></div>
      <div class="field"><label class="field__label">Workspace <span class="req">*</span></label><WorkspaceSelect v-model="addForm.workspace_id" :allow-all="false" all-label="— выбери —"/></div>
      <div v-if="addForm.source_type==='telegram'" class="field-row">
        <div class="field"><label class="field__label">Telegram-канал <span class="req">*</span></label><input class="input" v-model="addForm.tg_channel" placeholder="@channel"></div>
        <div class="field" style="max-width:140px"><label class="field__label">Аккаунт #</label><input class="input" type="number" v-model="addForm.tg_account_idx" min="0"></div>
      </div>
      <div v-else class="field"><label class="field__label">URL <span class="req">*</span></label><input class="input mono" v-model="addForm.url" placeholder="https://…"></div>
      <div class="field"><label class="field__label">Cron (опц.)</label><input class="input mono" v-model="addForm.schedule_cron" placeholder="*/30 * * * *"><span class="field__hint">Пусто — интервал по умолчанию для типа</span></div>
      <details>
        <summary class="muted text-sm" style="cursor:pointer;margin-bottom:8px">Дополнительно (proxy_config / extra, JSON)</summary>
        <div class="field"><label class="field__label">proxy_config</label><textarea class="textarea" v-model="addForm.proxy_config" placeholder='{"host":"…","port":1080}'></textarea></div>
        <div class="field"><label class="field__label">extra</label><textarea class="textarea" v-model="addForm.extra" placeholder='{"authority":0.8}'></textarea></div>
      </details>
      <div v-if="addErr" class="field__error">{{ addErr }}</div>
      <template #footer>
        <button class="btn btn--outline" @click="addOpen = false">Отмена</button>
        <button class="btn btn--primary" :disabled="busy" @click="submitAdd"><span v-if="busy" class="spin"></span> Сохранить</button>
      </template>
    </UiModal>

    <!-- Vision -->
    <UiModal v-if="visionOpen" :title="'Vision: ' + (visionSrc.name || visionSrc.id)" @close="visionOpen = false">
      <div class="field"><label class="field__label">Режим обработки медиа</label>
        <select class="select" v-model="visionForm.mode"><option v-for="m in visionModes" :key="m.v" :value="m.v">{{ m.l }}</option></select>
      </div>
      <div class="field"><label class="field__label">Лимит размера медиа (байт)</label><input class="input mono" type="number" v-model="visionForm.max_media_bytes"><span class="field__hint">Медиа крупнее лимита пропускается</span></div>
      <template #footer>
        <button class="btn btn--outline" @click="visionOpen = false">Отмена</button>
        <button class="btn btn--primary" :disabled="busy" @click="submitVision">Сохранить</button>
      </template>
    </UiModal>

    <!-- Telegram handle -->
    <UiModal v-if="tgOpen" :title="'Сменить канал: ' + (tgSrc.name || tgSrc.id)" @close="tgOpen = false">
      <div class="field"><label class="field__label">Новый Telegram-канал</label><input class="input" v-model="tgForm.tg_channel" placeholder="@channel"></div>
      <div class="badge badge--warn" style="display:block;padding:10px 12px;border-radius:9px">⚠ Смена канала сбрасывает checkpoint-курсор — сбор пойдёт заново с начала.</div>
      <template #footer>
        <button class="btn btn--outline" @click="tgOpen = false">Отмена</button>
        <button class="btn btn--primary" :disabled="busy" @click="submitTg">Сменить</button>
      </template>
    </UiModal>
  </div>
  `,
  created() { this.visionModes = VISION_MODES; },
};
