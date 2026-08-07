import { api } from '../api.js';
import { fmtAgo, fmtNum, statusVariant, truncate } from '../format.js';
import { newSequence } from '../reqseq.js';

// Порядок и русские подписи для статусов индексации — приводим сырые ключи к читаемому виду.
const STATUS_ORDER = ['done', 'indexed', 'processing', 'pending', 'queued', 'skipped', 'error', 'failed', 'dropped'];
const STATUS_LABELS = {
  done: 'Готово', indexed: 'Проиндексировано', processing: 'В обработке',
  pending: 'В очереди', queued: 'В очереди', skipped: 'Пропущено',
  error: 'Ошибка', failed: 'Ошибка', dropped: 'Отброшено', empty: 'Пусто',
};

export default {
  name: 'DashboardView',
  data() {
    return {
      // Сторож устаревших ответов, см. js/reqseq.js
      seq: newSequence(),
      stats: {}, recent: [], loading: true, error: null,
      workspace: this.$route.query.ws || '',
    };
  },
  computed: {
    statCards() {
      const entries = Object.entries(this.stats || {});
      entries.sort((a, b) => {
        const ia = STATUS_ORDER.indexOf(a[0]); const ib = STATUS_ORDER.indexOf(b[0]);
        return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib);
      });
      return entries.map(([status, v]) => ({
        status,
        label: STATUS_LABELS[status] || status,
        count: (v && v.count) != null ? v.count : 0,
        avgScore: v && v.avg_score != null ? Number(v.avg_score) : null,
        variant: statusVariant(status),
      }));
    },
    totalCount() {
      return this.statCards.reduce((s, c) => s + (Number(c.count) || 0), 0);
    },
  },
  watch: {
    workspace(v) {
      this.$router.replace({ query: { ...this.$route.query, ws: v || undefined } });
      this.load();
    },
  },
  mounted() { this.load(); },
  methods: {
    fmtAgo, fmtNum, statusVariant, truncate,
    statusLabel(s) { return STATUS_LABELS[s] || s; },
    fmtScore(v) { return v == null || isNaN(v) ? '—' : Number(v).toFixed(2); },
    async load() {
      const fresh = this.seq.next();
      this.loading = true; this.error = null;
      try {
        const data = await api.get('/api/pipeline/stats', { workspace_id: this.workspace || undefined });
        if (!fresh()) return;
        this.stats = (data && data.stats) || {};
        this.recent = (data && Array.isArray(data.recent)) ? data.recent : [];
      } catch (e) { if (!fresh()) return; this.error = e; }
      finally { if (fresh()) this.loading = false; }
    },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Обзор пайплайна</h1>
        <p>{{ fmtNum(totalCount) }} постов в статистике · последние {{ recent.length }} на индексации</p>
      </div>
      <div class="page-head__actions">
        <router-link to="/pipeline" class="btn btn--outline btn--sm">Пайплайн →</router-link>
        <button class="btn btn--outline" @click="load" :disabled="loading">↻ Обновить</button>
      </div>
    </div>

    <div class="toolbar">
      <div class="toolbar__group"><label>Workspace</label><WorkspaceSelect v-model="workspace"/></div>
    </div>

    <StateBlock :loading="loading" :error="error" :empty="!statCards.length && !recent.length"
      empty-text="Пока нет данных пайплайна" @retry="load">

      <div v-if="statCards.length" class="grid grid--stats" style="margin-bottom:var(--sp-4)">
        <div v-for="c in statCards" :key="c.status" class="stat">
          <div class="stat__label">
            <UiBadge :variant="c.variant" :text="c.label"/>
          </div>
          <div class="stat__value">{{ fmtNum(c.count) }}</div>
          <div class="stat__meta">avg score: {{ fmtScore(c.avgScore) }}</div>
        </div>
      </div>

      <div class="card">
        <div class="card__head"><h2>Последние посты</h2><span class="sub">клик по строке — к постам</span></div>
        <div class="card__body--flush">
          <StateBlock :empty="!recent.length" empty-text="Постов пока нет">
            <div class="table-wrap">
              <table class="tbl tbl--fixed">
                <colgroup>
                  <col style="width:14%"><col style="width:34%"><col style="width:15%">
                  <col style="width:11%"><col style="width:14%"><col style="width:12%">
                </colgroup>
                <thead><tr>
                  <th>Источник</th><th>Превью</th><th>Категория</th>
                  <th class="num">Релевантность</th><th>Статус</th><th>Время</th>
                </tr></thead>
                <tbody>
                  <tr v-for="p in recent" :key="p.id"
                      style="cursor:pointer"
                      @click="$router.push({ path: '/posts', query: { ws: p.workspace_id || workspace || undefined } })">
                    <td>
                      <div class="ellip mono text-sm" :title="p.source_id">{{ p.source_id || '—' }}</div>
                      <div class="text-xs faint ellip" :title="p.workspace_id">{{ p.workspace_id || '' }}</div>
                    </td>
                    <td class="muted"><span class="ellip" style="display:block" :title="p.preview">{{ p.preview || '—' }}</span></td>
                    <td><span class="ellip" style="display:block" :title="p.category">{{ p.category || '—' }}</span></td>
                    <td class="num mono">{{ p.relevance_score != null ? Number(p.relevance_score).toFixed(2) : '—' }}</td>
                    <td>
                      <div class="row" style="gap:6px;flex-wrap:wrap">
                        <UiBadge v-if="p.embedding_status" :variant="statusVariant(p.embedding_status)" :text="p.embedding_status" :dot="false"/>
                        <UiBadge v-if="p.vision_status" :variant="statusVariant(p.vision_status)" :text="p.vision_status" :dot="false"/>
                      </div>
                    </td>
                    <td class="muted text-sm">{{ fmtAgo(p.created_at || p.published_at || p.processed_at) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </StateBlock>
        </div>
      </div>

    </StateBlock>
  </div>
  `,
};
