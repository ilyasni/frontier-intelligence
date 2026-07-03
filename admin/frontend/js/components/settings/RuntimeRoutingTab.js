import { api } from '../../api.js';
import { notify, confirmDialog } from '../../ui.js';
import SettingCard from './SettingCard.js';
import KvGrid from './KvGrid.js';
import {
  fmtTs, toneForStatus, uniqueStrings,
  POLICY_FAMILIES, POLICY_MODES, SIM_TASKS, familyForTask,
} from './helpers.js';

// Вкладка «Runtime и роутинг»: runtime-mode, control-plane policy v2 + симулятор, legacy llm-routing (read-only).
export default {
  name: 'RuntimeRoutingTab',
  components: { SettingCard, KvGrid },
  data() {
    return {
      // runtime mode
      rm: null, rmLoading: true, rmError: null, rmSelected: '', rmBusy: false,
      // policy v2
      policy: null, policyLoading: true, policyError: null, policyBusy: false,
      editor: null, // редактируемая копия effective-политики
      // simulator
      simTask: 'relevance', simFamily: 'text_generation', simMode: '',
      simBusy: false, simResult: null, simError: null,
      // legacy routing (advanced)
      legacy: null, legacyLoading: false, legacyError: null, legacyOpen: false,
    };
  },
  computed: {
    rmOptions() { return (this.rm && Array.isArray(this.rm.options)) ? this.rm.options : []; },
    rmActive() { return this.rmOptions.find((o) => o.id === (this.rm && this.rm.mode)) || {}; },
    rmCells() {
      const p = this.rm || {};
      return [
        { label: 'Активный режим', value: p.mode || '—', meta: this.rmActive.description || 'Текущий runtime-режим', badge: p.mode === 'full-vision' ? 'success' : 'warn' },
        { label: 'Источник', value: p.source || 'env', meta: (p.source || 'env') === 'env' ? 'Оверрайд в БД не сохранён' : 'Хранится в Postgres + Redis' },
        { label: 'Обновлён', value: fmtTs(p.updated_at), meta: 'Последняя запись оверрайда' },
      ];
    },
    policySrc() { return this.policy ? (this.policy.source || 'derived') : 'derived'; },
    policyCells() {
      const eff = (this.policy && this.policy.effective) || {};
      const fams = POLICY_FAMILIES.map((f) => eff[f.key]).filter(Boolean);
      const totalCand = fams.reduce((s, f) => s + ((f.candidates || []).length), 0);
      const providers = uniqueStrings(fams.flatMap((f) => (f.candidates || []).map((c) => c.provider)));
      const shadow = fams.filter((f) => String(f.mode || '') === 'shadow-eval').length;
      return [
        { label: 'Версия', value: eff.version || 'v2', meta: 'Схема политики' },
        { label: 'Режим по умолчанию', value: eff.default_mode || 'degraded', meta: 'Используется семействами по умолчанию' },
        { label: 'Источник', value: this.policySrc, meta: this.policySrc === 'derived' ? 'Выведено из runtime-mode и legacy-роутинга' : 'Сохранённая v2-политика', badge: this.policySrc === 'derived' ? 'warn' : 'success' },
        { label: 'Кандидатов', value: String(totalCand), meta: 'Всего provider/model по семействам' },
        { label: 'Провайдеров', value: String(providers.length), meta: 'Уникальные провайдеры в политике' },
        { label: 'Shadow-семейств', value: String(shadow), meta: 'Семейства в режиме shadow-eval' },
      ];
    },
  },
  mounted() {
    this.loadRuntimeMode();
    this.loadPolicy();
  },
  methods: {
    fmtTs, toneForStatus,
    // ---------- Runtime mode ----------
    async loadRuntimeMode() {
      this.rmLoading = true; this.rmError = null;
      try {
        this.rm = await api.get('/api/settings/runtime-mode');
        this.rmSelected = this.rm.mode || '';
      } catch (e) { this.rmError = e; }
      finally { this.rmLoading = false; }
    },
    async applyRuntimeMode() {
      if (!this.rmSelected || this.rmSelected === (this.rm && this.rm.mode)) {
        notify.info('Режим не изменён'); return;
      }
      const ok = await confirmDialog({
        title: 'Сменить runtime-режим?',
        message: `Режим «${this.rmSelected}» повлияет на весь пайплайн (Vision, модели GigaChat, роутинг). Применить?`,
        confirmText: 'Применить', danger: true,
      });
      if (!ok) return;
      this.rmBusy = true;
      try {
        this.rm = await api.post('/api/settings/runtime-mode', { mode: this.rmSelected });
        this.rmSelected = this.rm.mode || '';
        notify.success('Runtime-режим применён', this.rm.mode);
        this.loadPolicy();
      } catch (e) { notify.error('Не удалось применить режим', e.detail); }
      finally { this.rmBusy = false; }
    },
    // ---------- Control-plane policy ----------
    async loadPolicy() {
      this.policyLoading = true; this.policyError = null;
      try {
        this.policy = await api.get('/api/settings/policy');
        this.buildEditor();
      } catch (e) { this.policyError = e; }
      finally { this.policyLoading = false; }
    },
    buildEditor() {
      const eff = (this.policy && this.policy.effective) || {};
      const clone = {
        version: 'v2',
        default_mode: eff.default_mode || 'degraded',
        text_generation: this.cloneFamily(eff.text_generation, 'text_generation'),
        vision_generation: this.cloneFamily(eff.vision_generation, 'vision_generation'),
        embeddings: this.cloneFamily(eff.embeddings, 'embeddings'),
      };
      this.editor = clone;
    },
    cloneFamily(fam, key) {
      fam = fam || {};
      return {
        family: fam.family || key,
        mode: fam.mode || 'degraded',
        fallback_exception_only: fam.fallback_exception_only !== false,
        candidates: (Array.isArray(fam.candidates) ? fam.candidates : []).map((c) => ({
          provider: c.provider || '', model: c.model || '',
          enabled: c.enabled !== false, budget_class: c.budget_class || '',
        })),
      };
    },
    familyLabel(key) { return (POLICY_FAMILIES.find((f) => f.key === key) || {}).label || key; },
    addCandidate(famKey) {
      this.editor[famKey].candidates.push({ provider: '', model: '', enabled: true, budget_class: '' });
    },
    removeCandidate(famKey, idx) {
      this.editor[famKey].candidates.splice(idx, 1);
    },
    policyValid() {
      for (const f of POLICY_FAMILIES) {
        const fam = this.editor[f.key];
        const cand = (fam.candidates || []).filter((c) => c.provider.trim() && c.model.trim());
        if (!cand.length) return `${f.label}: нужен хотя бы один кандидат с provider и model`;
        if (!cand.some((c) => c.enabled)) return `${f.label}: хотя бы один кандидат должен быть enabled`;
      }
      return '';
    },
    async savePolicy() {
      const err = this.policyValid();
      if (err) { notify.error('Политика невалидна', err); return; }
      const ok = await confirmDialog({
        title: 'Сохранить control-plane политику v2?',
        message: 'Новая политика роутинга применится ко всему пайплайну (все воркеры). Кандидаты без provider/model будут отброшены. Продолжить?',
        confirmText: 'Сохранить', danger: true,
      });
      if (!ok) return;
      const body = {
        version: 'v2',
        default_mode: this.editor.default_mode,
        text_generation: this.serializeFamily(this.editor.text_generation),
        vision_generation: this.serializeFamily(this.editor.vision_generation),
        embeddings: this.serializeFamily(this.editor.embeddings),
      };
      this.policyBusy = true;
      try {
        this.policy = await api.post('/api/settings/policy', body);
        this.buildEditor();
        notify.success('Политика сохранена');
      } catch (e) { notify.error('Не удалось сохранить политику', e.detail); }
      finally { this.policyBusy = false; }
    },
    serializeFamily(fam) {
      return {
        family: fam.family,
        mode: fam.mode,
        fallback_exception_only: !!fam.fallback_exception_only,
        candidates: (fam.candidates || [])
          .filter((c) => c.provider.trim() && c.model.trim())
          .map((c) => ({
            provider: c.provider.trim(), model: c.model.trim(),
            enabled: !!c.enabled, budget_class: c.budget_class.trim() || undefined,
          })),
      };
    },
    // ---------- Simulator ----------
    syncSimFamily() { this.simFamily = familyForTask(this.simTask); },
    async runSimulation() {
      this.simBusy = true; this.simError = null; this.simResult = null;
      try {
        this.simResult = await api.post('/api/settings/simulate', {
          task: this.simTask || 'relevance',
          task_family: this.simFamily || familyForTask(this.simTask),
          mode: this.simMode || undefined,
        });
      } catch (e) { this.simError = e; }
      finally { this.simBusy = false; }
    },
    simCells() {
      const p = this.simResult || {};
      const d = p.decision || {};
      const ps = Array.isArray(p.provider_states) ? p.provider_states : [];
      const skipped = Array.isArray(d.skipped_candidates) ? d.skipped_candidates : [];
      const degraded = ps.filter((i) => String(i.readiness_state || '').toLowerCase() !== 'ready').length;
      return [
        { label: 'Выбранный маршрут', value: `${d.selected_provider || '—'} / ${d.selected_model || '—'}`, meta: 'После проверок readiness/circuit/quota', badge: 'success' },
        { label: 'Запрошенный маршрут', value: `${d.requested_provider || '—'} / ${d.requested_model || '—'}`, meta: 'Первый кандидат по политике' },
        { label: 'Fallback', value: d.fallback_allowed ? 'разрешён' : 'выключен', meta: d.fallback_exception_only ? 'Только как исключение' : 'Может двигаться по цепочке' },
        { label: 'Пропущено', value: String(skipped.length), meta: 'Кандидаты исключены проверками' },
        { label: 'Провайдеров degraded', value: String(degraded), meta: 'Не в состоянии readiness=ready' },
        { label: 'Открытых circuits', value: String((p.circuits || []).length), meta: 'Учтено при симуляции' },
      ];
    },
    // ---------- Legacy routing ----------
    async toggleLegacy() {
      this.legacyOpen = !this.legacyOpen;
      if (this.legacyOpen && !this.legacy && !this.legacyLoading) {
        this.legacyLoading = true; this.legacyError = null;
        try { this.legacy = await api.get('/api/settings/llm-routing'); }
        catch (e) { this.legacyError = e; }
        finally { this.legacyLoading = false; }
      }
    },
  },
  template: `
  <div class="stack" style="gap:var(--sp-4)">

    <!-- Runtime mode -->
    <SettingCard title="Runtime-режим"
      desc="Живой роутинг пайплайна: тиры Vision и моделей GigaChat."
      :badge-text="rm ? (rm.mode || 'custom') : ''"
      :badge-variant="rm && rm.mode === 'full-vision' ? 'success' : 'warn'"
      :loading="rmLoading" :error="rmError" @retry="loadRuntimeMode">
      <div class="stack" style="gap:var(--sp-4)">
        <div class="toolbar" style="margin:0">
          <div class="toolbar__group" style="min-width:260px">
            <label>Режим</label>
            <select class="select" v-model="rmSelected">
              <option v-for="o in rmOptions" :key="o.id" :value="o.id">{{ o.label || o.id }}</option>
            </select>
          </div>
          <div class="toolbar__group"><label>&nbsp;</label>
            <button class="btn btn--primary" :disabled="rmBusy || rmSelected === (rm && rm.mode)" @click="applyRuntimeMode">
              <span v-if="rmBusy" class="spin"></span> Применить
            </button>
          </div>
          <div class="toolbar__group"><label>&nbsp;</label>
            <button class="btn btn--outline" :disabled="rmLoading" @click="loadRuntimeMode">↻ Обновить</button>
          </div>
        </div>
        <KvGrid :items="rmCells"/>
        <details v-if="rm && rm.effective">
          <summary class="muted text-sm" style="cursor:pointer">Эффективные настройки режима (raw)</summary>
          <div class="mt-2"><JsonView :data="rm.effective"/></div>
        </details>
      </div>
    </SettingCard>

    <!-- Control-plane policy v2 -->
    <SettingCard title="Control-plane политика v2"
      desc="Роутинг по task-family с явными цепочками кандидатов и режимами исполнения."
      :badge-text="policySrc" :badge-variant="policySrc === 'derived' ? 'warn' : 'success'"
      :loading="policyLoading" :error="policyError" @retry="loadPolicy">
      <div class="stack" style="gap:var(--sp-4)" v-if="editor">
        <KvGrid :items="policyCells"/>

        <div class="field" style="max-width:280px;margin:0">
          <label class="field__label">Режим по умолчанию</label>
          <select class="select" v-model="editor.default_mode">
            <option v-for="m in policyModes" :key="m" :value="m">{{ m }}</option>
          </select>
        </div>

        <div v-for="f in families" :key="f.key" class="card" style="box-shadow:none">
          <div class="card__head">
            <h2 style="font-size:var(--fs-sm)">{{ f.label }}</h2>
            <div class="spacer"></div>
            <div class="row" style="gap:var(--sp-3);flex-wrap:wrap">
              <div class="field" style="margin:0;min-width:150px">
                <select class="select" v-model="editor[f.key].mode">
                  <option v-for="m in policyModes" :key="m" :value="m">режим: {{ m }}</option>
                </select>
              </div>
              <label class="checkbox">
                <input type="checkbox" v-model="editor[f.key].fallback_exception_only"> fallback только как исключение
              </label>
            </div>
          </div>
          <div class="card__body--flush">
            <div class="table-wrap">
              <table class="tbl tbl--fixed" style="min-width:640px">
                <colgroup><col style="width:22%"><col style="width:34%"><col style="width:20%"><col style="width:12%"><col style="width:12%"></colgroup>
                <thead><tr><th>Провайдер</th><th>Модель</th><th>Budget class</th><th>Enabled</th><th></th></tr></thead>
                <tbody>
                  <tr v-for="(c, i) in editor[f.key].candidates" :key="i">
                    <td><input class="input mono" v-model="c.provider" placeholder="wormsoft"></td>
                    <td><input class="input mono" v-model="c.model" placeholder="gpt-4o-mini"></td>
                    <td><input class="input mono" v-model="c.budget_class" placeholder="opt."></td>
                    <td><label class="checkbox"><input type="checkbox" v-model="c.enabled"></label></td>
                    <td class="actions"><button class="btn btn--sm btn--danger" @click="removeCandidate(f.key, i)" title="Удалить">✕</button></td>
                  </tr>
                  <tr v-if="!editor[f.key].candidates.length"><td colspan="5" class="muted text-sm" style="text-align:center">Кандидатов нет — добавь хотя бы одного</td></tr>
                </tbody>
              </table>
            </div>
            <div style="padding:var(--sp-3)">
              <button class="btn btn--sm btn--outline" @click="addCandidate(f.key)">+ Кандидат</button>
            </div>
          </div>
        </div>

        <div class="row" style="gap:var(--sp-2)">
          <button class="btn btn--primary" :disabled="policyBusy" @click="savePolicy"><span v-if="policyBusy" class="spin"></span> Сохранить политику</button>
          <button class="btn btn--outline" :disabled="policyLoading" @click="loadPolicy">Сбросить к effective</button>
        </div>

        <details>
          <summary class="muted text-sm" style="cursor:pointer">Effective policy payload (raw)</summary>
          <div class="mt-2"><JsonView :data="policy.effective"/></div>
        </details>
      </div>
    </SettingCard>

    <!-- Route simulator -->
    <SettingCard title="Симулятор маршрута"
      desc="Прогон выбора маршрута по текущей политике: что выбрано и что отброшено."
      badge-text="simulate" badge-variant="accent">
      <div class="stack" style="gap:var(--sp-4)">
        <div class="toolbar" style="margin:0">
          <div class="toolbar__group" style="min-width:200px"><label>Task</label>
            <select class="select" v-model="simTask" @change="syncSimFamily">
              <option v-for="t in simTasks" :key="t.id" :value="t.id">{{ t.label }}</option>
            </select>
          </div>
          <div class="toolbar__group" style="min-width:200px"><label>Task family</label>
            <select class="select" v-model="simFamily">
              <option v-for="f in families" :key="f.key" :value="f.key">{{ f.key }}</option>
            </select>
          </div>
          <div class="toolbar__group" style="min-width:200px"><label>Mode override</label>
            <select class="select" v-model="simMode">
              <option value="">наследовать из политики</option>
              <option v-for="m in policyModes" :key="m" :value="m">{{ m }}</option>
            </select>
          </div>
          <div class="toolbar__group"><label>&nbsp;</label>
            <button class="btn btn--primary" :disabled="simBusy" @click="runSimulation"><span v-if="simBusy" class="spin"></span> Симулировать</button>
          </div>
        </div>

        <StateBlock :loading="simBusy" :error="simError" :empty="!simResult && !simBusy"
          empty-text="Выбери task и запусти симуляцию" @retry="runSimulation">
          <div class="stack" style="gap:var(--sp-4)" v-if="simResult">
            <KvGrid :items="simCells()"/>
            <div v-if="(simResult.decision && simResult.decision.skipped_candidates || []).length">
              <div class="muted text-sm mb-3">Пропущенные кандидаты</div>
              <div class="table-wrap">
                <table class="tbl">
                  <thead><tr><th>Кандидат</th><th>Причина</th></tr></thead>
                  <tbody>
                    <tr v-for="(s, i) in simResult.decision.skipped_candidates" :key="i">
                      <td class="mono">{{ s.provider || '—' }}/{{ s.model || '—' }}</td>
                      <td class="muted">{{ s.reason || '—' }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
            <details><summary class="muted text-sm" style="cursor:pointer">Raw результат симуляции</summary>
              <div class="mt-2"><JsonView :data="simResult"/></div>
            </details>
          </div>
        </StateBlock>
      </div>
    </SettingCard>

    <!-- Legacy llm-routing (advanced, read-only) -->
    <div class="card">
      <div class="card__head">
        <div style="flex:1"><h2>Legacy LLM-routing</h2><div class="sub">Унаследованный per-task роутинг — только для справки (управление через политику v2 выше).</div></div>
        <button class="btn btn--outline btn--sm" @click="toggleLegacy">{{ legacyOpen ? 'Скрыть' : 'Показать' }}</button>
      </div>
      <div class="card__body" v-if="legacyOpen">
        <StateBlock :loading="legacyLoading" :error="legacyError" :empty="!legacy && !legacyLoading" @retry="toggleLegacy">
          <JsonView v-if="legacy" :data="legacy"/>
        </StateBlock>
      </div>
    </div>

  </div>
  `,
  created() {
    this.policyModes = POLICY_MODES;
    this.families = POLICY_FAMILIES;
    this.simTasks = SIM_TASKS;
  },
};
