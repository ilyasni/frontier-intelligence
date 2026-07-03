import { api } from '../../api.js';
import { notify, confirmDialog } from '../../ui.js';
import SettingCard from './SettingCard.js';
import KvGrid from './KvGrid.js';
import { fmtTs, fmtPct01, toneForStatus } from './helpers.js';

// Вкладка «Xray»: health + probe, профили, история ремедиации, мутации failover/switch/rollback.
// Мутации влияют на egress — все через confirmDialog.
export default {
  name: 'XrayTab',
  components: { SettingCard, KvGrid },
  data() {
    return {
      health: { data: null, loading: true, error: null },
      history: { data: null, loading: true, error: null },
      profiles: { data: null, loading: true, error: null },
      remediation: { data: null, loading: true, error: null },
      probeBusy: false, busy: false,
      switchTarget: '',
    };
  },
  computed: {
    healthData() { return this.health.data || {}; },
    healthCells() {
      const d = this.healthData;
      const failed = Number(d.targets_failed || 0);
      const total = Number(d.targets_total || 0);
      const ratio = Number(d.failure_ratio || 0);
      const streak = Number(d.streak || 0);
      return [
        { label: 'Статус', value: String(d.status || 'unknown').toUpperCase(), meta: 'Общее состояние egress-контура', badge: toneForStatus(d.status) },
        { label: 'Отказов', value: `${failed}/${total}`, meta: 'Целей не прошли probe', badge: failed ? 'warn' : 'success' },
        { label: 'Failure ratio', value: fmtPct01(ratio), meta: 'Доля упавших целей', badge: ratio >= 0.66 ? 'danger' : (ratio > 0 ? 'warn' : 'success') },
        { label: 'Streak', value: String(streak), meta: 'Подряд неуспешных проверок', badge: streak >= 2 ? 'warn' : 'success' },
      ];
    },
    healthResults() { return Array.isArray(this.healthData.results) ? this.healthData.results : []; },
    profileRows() { return (this.profiles.data && Array.isArray(this.profiles.data.profiles)) ? this.profiles.data.profiles : []; },
    activeProfile() { return this.profiles.data && this.profiles.data.active_profile; },
    previousProfile() { return this.profiles.data && this.profiles.data.previous_profile; },
    remediationRows() { return (this.remediation.data && Array.isArray(this.remediation.data.history)) ? this.remediation.data.history : []; },
    historyRows() { return (this.history.data && Array.isArray(this.history.data.history)) ? this.history.data.history : []; },
    switchOptions() {
      return this.profileRows()
        .filter((p) => p.name && p.name !== this.activeProfile)
        .map((p) => p.name);
    },
  },
  mounted() { this.loadAll(); },
  methods: {
    fmtTs, fmtPct01, toneForStatus,
    async loadSection(key, path) {
      this[key] = { data: this[key].data, loading: true, error: null };
      try { this[key] = { data: await api.get(path), loading: false, error: null }; }
      catch (e) { this[key] = { data: null, loading: false, error: e }; }
    },
    loadAll() {
      this.loadSection('health', '/api/monitoring/xray/health');
      this.loadSection('history', '/api/monitoring/xray/health/history?limit=15');
      this.loadSection('profiles', '/api/monitoring/xray/profiles');
      this.loadSection('remediation', '/api/monitoring/xray/remediation/history');
    },
    async runProbe() {
      this.probeBusy = true;
      try {
        const data = await api.post('/api/monitoring/xray/health/run');
        this.health = { data, loading: false, error: null };
        notify.success('Probe выполнен', String(data.status || '').toUpperCase());
        this.loadSection('history', '/api/monitoring/xray/health/history?limit=15');
      } catch (e) { notify.error('Probe не удался', e.detail); }
      finally { this.probeBusy = false; }
    },
    async failover() {
      const ok = await confirmDialog({
        title: 'Xray failover на следующий профиль?',
        message: 'Переключит egress на следующий включённый профиль. Влияет на ВЕСЬ исходящий трафик (краулинг, LLM-прокси). Продолжить?',
        confirmText: 'Failover', danger: true,
      });
      if (!ok) return;
      this.busy = true;
      try {
        const res = await api.post('/api/monitoring/xray/remediate/failover', { reason: 'manual_failover' });
        notify.success('Failover выполнен', res.active_profile ? `Активен: ${res.active_profile}` : '');
        this.afterRemediation();
      } catch (e) { notify.error('Failover не удался', e.detail); }
      finally { this.busy = false; }
    },
    async switchProfile() {
      if (!this.switchTarget) { notify.info('Выбери целевой профиль'); return; }
      const ok = await confirmDialog({
        title: `Переключить egress на «${this.switchTarget}»?`,
        message: 'Смена активного xray-профиля влияет на весь исходящий трафик. Продолжить?',
        confirmText: 'Переключить', danger: true,
      });
      if (!ok) return;
      this.busy = true;
      try {
        const res = await api.post('/api/monitoring/xray/remediate/switch', { profile_name: this.switchTarget, reason: 'manual_switch' });
        notify.success('Профиль переключён', res.active_profile ? `Активен: ${res.active_profile}` : this.switchTarget);
        this.switchTarget = '';
        this.afterRemediation();
      } catch (e) { notify.error('Не удалось переключить', e.detail); }
      finally { this.busy = false; }
    },
    async rollback() {
      const ok = await confirmDialog({
        title: 'Откатить xray-профиль?',
        message: `Вернёт предыдущий активный профиль${this.previousProfile ? ` («${this.previousProfile}»)` : ''}. Влияет на весь egress. Продолжить?`,
        confirmText: 'Откатить', danger: true,
      });
      if (!ok) return;
      this.busy = true;
      try {
        const res = await api.post('/api/monitoring/xray/remediate/rollback', { reason: 'manual_rollback' });
        notify.success('Откат выполнен', res.active_profile ? `Активен: ${res.active_profile}` : '');
        this.afterRemediation();
      } catch (e) { notify.error('Откат не удался', e.detail); }
      finally { this.busy = false; }
    },
    afterRemediation() {
      this.loadSection('profiles', '/api/monitoring/xray/profiles');
      this.loadSection('remediation', '/api/monitoring/xray/remediation/history');
      this.loadSection('health', '/api/monitoring/xray/health');
    },
  },
  template: `
  <div class="stack" style="gap:var(--sp-4)">
    <div class="badge badge--warn" style="display:block;padding:10px 14px;border-radius:var(--r-md)">
      ⚠ Действия ремедиации (failover / switch / rollback) меняют активный xray-профиль и влияют на ВЕСЬ исходящий трафик стека.
    </div>

    <!-- Health -->
    <SettingCard title="Xray health"
      desc="Прокси-контур egress: детект деградации и сигнал для автореакции."
      :badge-text="healthData.status ? String(healthData.status).toUpperCase() : ''"
      :badge-variant="toneForStatus(healthData.status)"
      :loading="health.loading" :error="health.error"
      @retry="loadSection('health', '/api/monitoring/xray/health')">
      <template #head-actions>
        <button class="btn btn--outline btn--sm" :disabled="probeBusy" @click="runProbe"><span v-if="probeBusy" class="spin"></span> Прогнать probe</button>
      </template>
      <div class="stack" style="gap:var(--sp-4)">
        <KvGrid :items="healthCells"/>
        <div v-if="healthResults.length">
          <div class="muted text-sm mb-3">Цели проверки</div>
          <div class="table-wrap">
            <table class="tbl tbl--fixed" style="min-width:640px">
              <colgroup><col style="width:52%"><col style="width:14%"><col style="width:12%"><col style="width:22%"></colgroup>
              <thead><tr><th>Цель</th><th>Состояние</th><th class="num">Код</th><th>Ошибка</th></tr></thead>
              <tbody>
                <tr v-for="(r, i) in healthResults" :key="i">
                  <td class="mono ellip" :title="r.url">{{ r.url || '—' }}</td>
                  <td><UiBadge :variant="r.ok ? 'success' : 'danger'" :text="r.ok ? 'ok' : 'fail'"/></td>
                  <td class="num">{{ r.status_code == null ? '—' : r.status_code }}</td>
                  <td class="muted ellip" :title="r.error">{{ r.error || '—' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </SettingCard>

    <!-- Profiles + remediation actions -->
    <SettingCard title="Xray-профили"
      desc="Реестр egress-профилей и действия ремедиации."
      :badge-text="activeProfile || ''" :badge-variant="activeProfile ? 'accent' : 'neutral'"
      :loading="profiles.loading" :error="profiles.error"
      @retry="loadSection('profiles', '/api/monitoring/xray/profiles')">
      <div class="stack" style="gap:var(--sp-4)">
        <div class="dl">
          <dt>Активный</dt><dd class="mono">{{ activeProfile || '—' }}</dd>
          <dt>Предыдущий</dt><dd class="mono">{{ previousProfile || '—' }}</dd>
          <dt>Режим</dt><dd>{{ (profiles.data && profiles.data.mode) || '—' }}</dd>
        </div>

        <div class="table-wrap" v-if="profileRows().length">
          <table class="tbl">
            <thead><tr><th>Профиль</th><th>Активен</th><th>Включён</th><th>Детали</th></tr></thead>
            <tbody>
              <tr v-for="(p, i) in profileRows()" :key="i">
                <td class="mono">{{ p.name || '—' }}</td>
                <td><UiBadge v-if="p.is_active || p.name === activeProfile" variant="success" text="активен"/><span v-else class="faint">—</span></td>
                <td><UiBadge :variant="p.enabled === false ? 'neutral' : 'success'" :text="p.enabled === false ? 'выкл' : 'вкл'" :dot="false"/></td>
                <td class="muted ellip" :title="p.tag || p.description || p.outbound || ''">{{ p.tag || p.description || p.outbound || '—' }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div class="toolbar" style="margin:0">
          <div class="toolbar__group" style="min-width:220px">
            <label>Переключить на профиль</label>
            <select class="select" v-model="switchTarget">
              <option value="">— выбери профиль —</option>
              <option v-for="n in switchOptions" :key="n" :value="n">{{ n }}</option>
            </select>
          </div>
          <div class="toolbar__group"><label>&nbsp;</label>
            <button class="btn btn--primary" :disabled="busy || !switchTarget" @click="switchProfile">Переключить</button>
          </div>
          <div class="toolbar__group"><label>&nbsp;</label>
            <button class="btn btn--outline" :disabled="busy" @click="failover">Failover ↦ следующий</button>
          </div>
          <div class="toolbar__group"><label>&nbsp;</label>
            <button class="btn btn--danger" :disabled="busy || !previousProfile" @click="rollback">Откат к предыдущему</button>
          </div>
        </div>
        <details><summary class="muted text-sm" style="cursor:pointer">Raw profiles</summary><div class="mt-2"><JsonView :data="profiles.data"/></div></details>
      </div>
    </SettingCard>

    <!-- Remediation history -->
    <SettingCard title="История ремедиации"
      desc="Последние переключения профилей (failover/switch/rollback)."
      :badge-text="remediationRows().length ? remediationRows().length + ' записей' : ''"
      :badge-variant="remediationRows().length ? 'neutral' : 'neutral'"
      :loading="remediation.loading" :error="remediation.error"
      :empty="!remediationRows().length" empty-text="Истории ремедиации нет"
      @retry="loadSection('remediation', '/api/monitoring/xray/remediation/history')">
      <div class="table-wrap">
        <table class="tbl tbl--fixed" style="min-width:720px">
          <thead><tr><th>Когда</th><th>Триггер</th><th>Профиль</th><th>Причина</th><th>Статус</th></tr></thead>
          <tbody>
            <tr v-for="(r, i) in remediationRows()" :key="i">
              <td class="muted">{{ fmtTs(r.created_at || r.timestamp || r.checked_at) }}</td>
              <td class="mono">{{ r.trigger || '—' }}</td>
              <td class="mono ellip" :title="(r.active_profile || '') + ' ← ' + (r.previous_profile || '')">{{ r.active_profile || r.target_profile || '—' }}</td>
              <td class="muted ellip" :title="r.reason">{{ r.reason || '—' }}</td>
              <td><UiBadge :variant="toneForStatus(r.status)" :text="r.status || '—'" :dot="false"/></td>
            </tr>
          </tbody>
        </table>
      </div>
    </SettingCard>

    <!-- Health history -->
    <SettingCard title="История probe"
      desc="Недавние прогоны health-check egress-контура."
      :loading="history.loading" :error="history.error"
      :empty="!historyRows().length" empty-text="Истории probe нет"
      @retry="loadSection('history', '/api/monitoring/xray/health/history?limit=15')">
      <div class="table-wrap">
        <table class="tbl tbl--fixed" style="min-width:680px">
          <thead><tr><th>Когда</th><th>Статус</th><th class="num">Отказов</th><th class="num">Ratio</th><th class="num">Streak</th><th>Alert</th></tr></thead>
          <tbody>
            <tr v-for="(h, i) in historyRows()" :key="i">
              <td class="muted">{{ fmtTs(h.checked_at) }}</td>
              <td><UiBadge :variant="toneForStatus(h.status)" :text="h.status || '—'" :dot="false"/></td>
              <td class="num">{{ Number(h.targets_failed || 0) }}/{{ Number(h.targets_total || 0) }}</td>
              <td class="num">{{ fmtPct01(h.failure_ratio) }}</td>
              <td class="num">{{ Number(h.streak || 0) }}</td>
              <td><UiBadge :variant="h.alert_sent ? 'warn' : 'success'" :text="h.alert_sent ? 'отправлен' : 'нет'" :dot="false"/></td>
            </tr>
          </tbody>
        </table>
      </div>
    </SettingCard>

  </div>
  `,
};
