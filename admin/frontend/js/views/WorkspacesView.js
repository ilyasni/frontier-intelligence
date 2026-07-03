import { api } from '../api.js';
import { notify, confirmDialog } from '../ui.js';
import { loadWorkspaces } from '../store.js';
import { fmtDate } from '../format.js';

// Порог релевантности лежит внутри relevance_weights.threshold. Значение по умолчанию — 0.6
// (как в legacy _parseWsThreshold). relevance_weights может прийти строкой — парсим на всякий.
function parseThreshold(ws) {
  let rw = ws && ws.relevance_weights;
  if (typeof rw === 'string') {
    try { rw = JSON.parse(rw); } catch { rw = {}; }
  }
  if (rw && typeof rw === 'object' && rw.threshold != null && rw.threshold !== '') {
    const n = parseFloat(rw.threshold);
    if (!isNaN(n)) return n;
  }
  return 0.6;
}

const asArray = (v) => (Array.isArray(v) ? v : []);
const splitCsv = (v) => String(v || '').split(',').map((s) => s.trim()).filter(Boolean);

export default {
  name: 'WorkspacesView',
  data() {
    return {
      workspaces: [], loading: true, error: null, busy: false,
      q: '',
      detail: null,
      formOpen: false, editMode: false, form: this.blankForm(), formErr: '',
    };
  },
  computed: {
    filtered() {
      const q = this.q.trim().toLowerCase();
      if (!q) return this.workspaces;
      return this.workspaces.filter((w) => {
        const hay = `${w.name || ''} ${w.id || ''} ${w.description || ''}`.toLowerCase();
        return hay.includes(q);
      });
    },
    counts() {
      return {
        total: this.workspaces.length,
        active: this.workspaces.filter((w) => w.is_active).length,
      };
    },
  },
  mounted() { this.load(); },
  methods: {
    fmtDate,
    parseThreshold,
    catCount(w) { return asArray(w.categories).length; },
    async load() {
      this.loading = true; this.error = null;
      try {
        const data = await api.get('/api/workspaces');
        this.workspaces = Array.isArray(data) ? data : [];
      } catch (e) { this.error = e; }
      finally { this.loading = false; }
    },
    // Стор workspaces кэшируется и переиспользуется глобальным <WorkspaceSelect> —
    // после любой мутации обновляем его принудительно, чтобы фильтры не устарели.
    refreshStore() { loadWorkspaces(true).catch(() => {}); },
    blankForm() {
      return {
        id: '', name: '', description: '',
        categories: 'technology, design, business_models',
        relevance_threshold: '0.6',
        design_lenses: '', cross_workspace_bridges: '', extra: '',
      };
    },
    parseJsonField(v, label) {
      if (!v || !v.trim()) return {};
      try { const o = JSON.parse(v); if (o && typeof o === 'object' && !Array.isArray(o)) return o; throw 0; }
      catch { throw new Error(`${label}: невалидный JSON-объект`); }
    },
    // ---- Создание / редактирование ----
    openCreate() {
      this.editMode = false;
      this.form = this.blankForm();
      this.formErr = '';
      this.formOpen = true;
    },
    async openEdit(w) {
      // Тянем свежую версию по id — карточка списка может быть неполной.
      let ws = w;
      try { ws = await api.get(`/api/workspaces/${encodeURIComponent(w.id)}`); }
      catch (e) { notify.error('Не удалось загрузить workspace', e.detail); return; }
      this.editMode = true;
      this.form = {
        id: ws.id,
        name: ws.name || '',
        description: ws.description || '',
        categories: asArray(ws.categories).join(', '),
        relevance_threshold: String(parseThreshold(ws)),
        design_lenses: asArray(ws.design_lenses).join(', '),
        cross_workspace_bridges: asArray(ws.cross_workspace_bridges).join(', '),
        extra: ws.extra && Object.keys(ws.extra).length ? JSON.stringify(ws.extra, null, 2) : '',
      };
      this.formErr = '';
      this.formOpen = true;
    },
    async submitForm() {
      this.formErr = '';
      const f = this.form;
      if (!this.editMode && !f.id.trim()) return (this.formErr = 'Укажи ID (slug) workspace');
      if (!f.name.trim()) return (this.formErr = 'Укажи имя');
      const thr = parseFloat(f.relevance_threshold);
      if (isNaN(thr) || thr < 0 || thr > 1) return (this.formErr = 'Порог релевантности — число от 0 до 1');
      let extra;
      try { extra = this.parseJsonField(f.extra, 'extra'); }
      catch (e) { return (this.formErr = e.message); }

      const payload = {
        name: f.name.trim(),
        description: f.description.trim(),
        categories: splitCsv(f.categories),
        relevance_threshold: thr,
        design_lenses: splitCsv(f.design_lenses),
        cross_workspace_bridges: splitCsv(f.cross_workspace_bridges),
        extra,
      };
      this.busy = true;
      try {
        if (this.editMode) {
          await api.patch(`/api/workspaces/${encodeURIComponent(f.id)}`, payload);
        } else {
          await api.post('/api/workspaces', { id: f.id.trim(), ...payload });
        }
        notify.success('Workspace сохранён', payload.name);
        this.formOpen = false;
        this.refreshStore();
        this.load();
      } catch (e) { this.formErr = e.detail || 'Ошибка сохранения'; }
      finally { this.busy = false; }
    },
    // ---- Toggle ----
    async toggle(w) {
      try {
        await api.patch(`/api/workspaces/${encodeURIComponent(w.id)}/toggle`);
        w.is_active = !w.is_active;
        notify.success(w.is_active ? 'Workspace включён' : 'Workspace выключен', w.name || w.id);
        this.refreshStore();
      } catch (e) { notify.error('Не удалось переключить', e.detail); }
    },
    // ---- Bootstrap ----
    async bootstrap() {
      const ok = await confirmDialog({
        title: 'Bootstrap из YAML?',
        message: 'Workspaces из config/workspaces.yml будут добавлены/обновлены (UPSERT, без удаления).',
        confirmText: 'Запустить',
      });
      if (!ok) return;
      this.busy = true;
      try {
        await api.post('/api/workspaces/bootstrap');
        notify.success('Bootstrap выполнен');
        this.refreshStore();
        this.load();
      } catch (e) { notify.error('Bootstrap не удался', e.detail); }
      finally { this.busy = false; }
    },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Workspaces</h1>
        <p>{{ counts.total }} всего · {{ counts.active }} активны</p>
      </div>
      <div class="page-head__actions">
        <button class="btn btn--outline btn--sm" :disabled="busy" @click="bootstrap">Bootstrap YAML</button>
        <button class="btn btn--primary" @click="openCreate">+ Новый</button>
      </div>
    </div>

    <div class="toolbar">
      <div class="toolbar__group" style="flex:1;min-width:200px"><label>Поиск</label>
        <input class="input" v-model="q" placeholder="имя, ID, описание…">
      </div>
      <div class="toolbar__group"><label>&nbsp;</label><button class="btn btn--outline" @click="load" :disabled="loading">↻ Обновить</button></div>
    </div>

    <div class="card"><div class="card__body--flush">
      <StateBlock :loading="loading" :error="error" :empty="!filtered.length"
        :empty-text="workspaces.length ? 'Нет workspace по фильтру' : 'Workspace пока нет'" @retry="load">
        <div class="table-wrap">
          <table class="tbl tbl--fixed">
            <colgroup>
              <col style="width:30%"><col style="width:32%"><col style="width:14%">
              <col style="width:10%"><col style="width:14%">
            </colgroup>
            <thead><tr>
              <th>Workspace</th><th>Категории</th><th class="num">Порог</th><th>Статус</th><th></th>
            </tr></thead>
            <tbody>
              <tr v-for="w in filtered" :key="w.id">
                <td>
                  <div class="ellip" style="font-weight:600;cursor:pointer" @click="detail = w" :title="w.name || w.id">{{ w.name || w.id }}</div>
                  <div class="row" style="gap:6px;margin-top:4px">
                    <span class="text-xs faint mono ellip">{{ w.id }}</span>
                  </div>
                  <div v-if="w.description" class="ellip text-xs muted" style="margin-top:4px" :title="w.description">{{ w.description }}</div>
                </td>
                <td>
                  <div class="row" style="gap:6px;flex-wrap:wrap" v-if="catCount(w)">
                    <UiBadge v-for="c in w.categories.slice(0, 3)" :key="c" variant="accent" :text="c" :dot="false"/>
                    <span v-if="catCount(w) > 3" class="text-xs faint">+{{ catCount(w) - 3 }}</span>
                  </div>
                  <span v-else class="muted">—</span>
                </td>
                <td class="num mono">{{ parseThreshold(w).toFixed(2) }}</td>
                <td>
                  <UiBadge :variant="w.is_active ? 'success' : 'neutral'" :text="w.is_active ? 'активен' : 'выкл'"/>
                </td>
                <td class="actions">
                  <button class="btn btn--sm btn--outline" @click="openEdit(w)">Редактировать</button>
                  <button class="btn btn--sm btn--ghost" @click="toggle(w)">{{ w.is_active ? 'Выкл' : 'Вкл' }}</button>
                  <button class="btn btn--sm btn--ghost" @click="detail = w" title="Детали">ⓘ</button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </StateBlock>
    </div></div>

    <!-- Детали -->
    <UiModal v-if="detail" :title="detail.name || detail.id" size="lg" @close="detail = null">
      <div class="dl">
        <dt>ID</dt><dd class="mono">{{ detail.id }}</dd>
        <dt>Имя</dt><dd>{{ detail.name || '—' }}</dd>
        <dt>Описание</dt><dd>{{ detail.description || '—' }}</dd>
        <dt>Порог релевантности</dt><dd class="mono">{{ parseThreshold(detail).toFixed(2) }}</dd>
        <dt>Статус</dt><dd><UiBadge :variant="detail.is_active ? 'success' : 'neutral'" :text="detail.is_active ? 'активен' : 'выкл'"/></dd>
        <dt>Создан</dt><dd>{{ detail.created_at ? fmtDate(detail.created_at) : '—' }}</dd>
        <dt>Обновлён</dt><dd>{{ detail.updated_at ? fmtDate(detail.updated_at) : '—' }}</dd>
      </div>

      <h4 class="mt-4 mb-3">Категории</h4>
      <div class="row" style="gap:6px;flex-wrap:wrap" v-if="catCount(detail)">
        <UiBadge v-for="c in detail.categories" :key="c" variant="accent" :text="c" :dot="false"/>
      </div>
      <div v-else class="muted text-sm">—</div>

      <template v-if="(detail.design_lenses || []).length">
        <h4 class="mt-4 mb-3">Design lenses</h4>
        <div class="row" style="gap:6px;flex-wrap:wrap">
          <UiBadge v-for="l in detail.design_lenses" :key="l" variant="info" :text="l" :dot="false"/>
        </div>
      </template>

      <template v-if="(detail.cross_workspace_bridges || []).length">
        <h4 class="mt-4 mb-3">Cross-workspace bridges</h4>
        <div class="row" style="gap:6px;flex-wrap:wrap">
          <UiBadge v-for="b in detail.cross_workspace_bridges" :key="b" :text="b" :dot="false"/>
        </div>
      </template>

      <h4 class="mt-4 mb-3">Relevance weights</h4>
      <JsonView :data="detail.relevance_weights || {}"/>

      <template v-if="detail.extra && Object.keys(detail.extra).length">
        <h4 class="mt-4 mb-3">Extra</h4>
        <JsonView :data="detail.extra"/>
      </template>

      <template #footer>
        <button class="btn btn--outline" @click="detail = null">Закрыть</button>
        <button class="btn btn--primary" @click="openEdit(detail); detail = null">Редактировать</button>
      </template>
    </UiModal>

    <!-- Создание / редактирование -->
    <UiModal v-if="formOpen" :title="editMode ? 'Редактировать workspace' : 'Новый workspace'" size="lg" @close="formOpen = false">
      <div class="field-row">
        <div class="field"><label class="field__label">ID (slug) <span class="req">*</span></label>
          <input class="input mono" v-model="form.id" :disabled="editMode" placeholder="my_workspace">
        </div>
        <div class="field"><label class="field__label">Порог релевантности</label>
          <input class="input mono" type="number" step="0.05" min="0" max="1" v-model="form.relevance_threshold">
          <span class="field__hint">0…1, по умолчанию 0.6</span>
        </div>
      </div>
      <div class="field"><label class="field__label">Имя <span class="req">*</span></label><input class="input" v-model="form.name" placeholder="Читаемое название"></div>
      <div class="field"><label class="field__label">Описание</label><textarea class="textarea" style="font-family:inherit;font-size:var(--fs-sm)" v-model="form.description" placeholder="Для чего этот workspace"></textarea></div>
      <div class="field"><label class="field__label">Категории</label>
        <input class="input" v-model="form.categories" placeholder="technology, design, business_models">
        <span class="field__hint">Через запятую — на бэк уйдут массивом</span>
      </div>
      <div class="field"><label class="field__label">Design lenses (опц.)</label>
        <input class="input" v-model="form.design_lenses" placeholder="через запятую">
      </div>
      <div class="field"><label class="field__label">Cross-workspace bridges (опц.)</label>
        <input class="input" v-model="form.cross_workspace_bridges" placeholder="ai_trends, design">
        <span class="field__hint">ID других workspace, через запятую</span>
      </div>
      <details>
        <summary class="muted text-sm" style="cursor:pointer;margin-bottom:8px">Дополнительно (extra, JSON)</summary>
        <div class="field"><label class="field__label">extra</label><textarea class="textarea" v-model="form.extra" placeholder='{"key":"value"}'></textarea></div>
      </details>
      <div v-if="formErr" class="field__error">{{ formErr }}</div>
      <template #footer>
        <button class="btn btn--outline" @click="formOpen = false">Отмена</button>
        <button class="btn btn--primary" :disabled="busy" @click="submitForm"><span v-if="busy" class="spin"></span> Сохранить</button>
      </template>
    </UiModal>
  </div>
  `,
};
