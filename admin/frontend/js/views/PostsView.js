import { api } from '../api.js';
import { fmtAgo, fmtDate, fmtNum, statusVariant, truncate } from '../format.js';

// Опции статуса пайплайна для фильтра (агрегированный status поста).
const STATUS_OPTIONS = [
  { v: 'pending', l: 'В очереди' },
  { v: 'processing', l: 'В обработке' },
  { v: 'done', l: 'Готово' },
  { v: 'error', l: 'Ошибка' },
  { v: 'skipped', l: 'Пропущено' },
];

export default {
  name: 'PostsView',
  data() {
    return {
      posts: [], loading: true, error: null,
      filters: {
        workspace: this.$route.query.ws || '',
        status: '', has_media: '', q: '', limit: 50,
      },
      detail: null, detailLoading: false, detailErr: null,
    };
  },
  computed: {
    filtered() {
      const q = this.filters.q.trim().toLowerCase();
      if (!q) return this.posts;
      return this.posts.filter((p) => {
        const hay = `${p.preview || ''} ${p.id || ''} ${p.category || ''}`.toLowerCase();
        return hay.includes(q);
      });
    },
  },
  watch: {
    'filters.workspace'(v) {
      this.$router.replace({ query: { ...this.$route.query, ws: v || undefined } });
      this.load();
    },
  },
  mounted() { this.load(); },
  methods: {
    fmtAgo, fmtDate, fmtNum, statusVariant, truncate,
    normLimit() {
      let n = Number(this.filters.limit) || 50;
      n = Math.max(1, Math.min(200, Math.floor(n)));
      this.filters.limit = n;
      return n;
    },
    async load() {
      this.loading = true; this.error = null;
      try {
        const data = await api.get('/api/posts', {
          workspace_id: this.filters.workspace || undefined,
          status: this.filters.status || undefined,
          has_media: this.filters.has_media || undefined,
          limit: this.normLimit(),
        });
        this.posts = Array.isArray(data) ? data : [];
      } catch (e) { this.error = e; }
      finally { this.loading = false; }
    },
    async openDetail(p) {
      this.detail = { post: p, enrichments: [], album: null };
      this.detailLoading = true; this.detailErr = null;
      try {
        const data = await api.get(`/api/posts/${encodeURIComponent(p.id)}`);
        this.detail = {
          post: (data && data.post) || p,
          enrichments: (data && data.enrichments) || [],
          album: (data && data.album) || null,
        };
      } catch (e) { this.detailErr = e; }
      finally { this.detailLoading = false; }
    },
    relevance(v) { return v != null && !isNaN(v) ? Number(v).toFixed(2) : '—'; },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Посты</h1>
        <p>{{ posts.length }} загружено · показано {{ filtered.length }}</p>
      </div>
    </div>

    <div class="toolbar">
      <div class="toolbar__group"><label>Workspace</label><WorkspaceSelect v-model="filters.workspace"/></div>
      <div class="toolbar__group"><label>Статус</label>
        <select class="select" v-model="filters.status" @change="load">
          <option value="">Любой</option>
          <option v-for="o in statusOptions" :key="o.v" :value="o.v">{{ o.l }}</option>
        </select>
      </div>
      <div class="toolbar__group"><label>Медиа</label>
        <select class="select" v-model="filters.has_media" @change="load">
          <option value="">Все</option>
          <option value="true">С медиа</option>
          <option value="false">Без медиа</option>
        </select>
      </div>
      <div class="toolbar__group" style="max-width:110px"><label>Лимит</label>
        <input class="input" type="number" min="1" max="200" v-model.number="filters.limit" @change="load">
      </div>
      <div class="toolbar__group" style="flex:1;min-width:200px"><label>Поиск</label>
        <input class="input" v-model="filters.q" placeholder="превью, категория, ID…">
      </div>
      <div class="toolbar__group"><label>&nbsp;</label><button class="btn btn--outline" @click="load" :disabled="loading">↻ Обновить</button></div>
    </div>

    <div class="card"><div class="card__body--flush">
      <StateBlock :loading="loading" :error="error" :empty="!filtered.length"
        :empty-text="posts.length ? 'Нет постов по фильтрам' : 'Постов пока нет'" @retry="load">
        <div class="table-wrap">
          <table class="tbl tbl--fixed">
            <colgroup>
              <col style="width:34%"><col style="width:12%"><col style="width:9%">
              <col style="width:9%"><col style="width:9%"><col style="width:9%"><col style="width:18%">
            </colgroup>
            <thead><tr>
              <th>Пост</th><th>Категория</th><th class="num">Relevance</th>
              <th>Embedding</th><th>Vision</th><th>Graph</th><th>Опубликовано</th>
            </tr></thead>
            <tbody>
              <tr v-for="p in filtered" :key="p.id" style="cursor:pointer" @click="openDetail(p)">
                <td>
                  <div class="row" style="gap:6px;margin-bottom:4px">
                    <span class="text-xs faint mono ellip" :title="p.id">{{ p.id }}</span>
                    <UiBadge v-if="p.has_media" variant="info" text="медиа" :dot="false"/>
                  </div>
                  <div class="ellip muted" :title="p.preview">{{ p.preview || '—' }}</div>
                </td>
                <td><span class="ellip" style="display:block" :title="p.category">{{ p.category || '—' }}</span></td>
                <td class="num mono">{{ relevance(p.relevance_score) }}</td>
                <td><UiBadge :variant="statusVariant(p.embedding_status)" :text="p.embedding_status || '—'" :dot="false"/></td>
                <td><UiBadge :variant="statusVariant(p.vision_status)" :text="p.vision_status || '—'" :dot="false"/></td>
                <td><UiBadge :variant="statusVariant(p.graph_status)" :text="p.graph_status || '—'" :dot="false"/></td>
                <td class="muted text-sm" :title="fmtDate(p.published_at)">{{ p.published_at ? fmtAgo(p.published_at) : '—' }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </StateBlock>
    </div></div>

    <!-- Detail -->
    <UiModal v-if="detail" :title="'Пост ' + (detail.post.id || '')" size="lg" @close="detail = null">
      <div class="dl">
        <dt>ID</dt><dd class="mono">{{ detail.post.id }}</dd>
        <dt>Workspace</dt><dd>{{ detail.post.workspace_id || '—' }}</dd>
        <dt>Источник</dt><dd class="mono">{{ detail.post.source_id || '—' }}</dd>
        <dt>External ID</dt><dd class="mono">{{ detail.post.external_id || '—' }}</dd>
        <dt>Категория</dt><dd>{{ detail.post.category || '—' }}</dd>
        <dt>Relevance</dt><dd class="mono">{{ relevance(detail.post.relevance_score) }}</dd>
        <dt>Медиа</dt><dd>{{ detail.post.has_media ? 'да' : 'нет' }}</dd>
        <dt>Статусы</dt>
        <dd>
          <div class="row" style="gap:6px;flex-wrap:wrap">
            <UiBadge :variant="statusVariant(detail.post.embedding_status)" text="embedding" :dot="false"/>
            <span class="text-xs muted mono">{{ detail.post.embedding_status || '—' }}</span>
            <UiBadge :variant="statusVariant(detail.post.vision_status)" text="vision" :dot="false"/>
            <span class="text-xs muted mono">{{ detail.post.vision_status || '—' }}</span>
            <UiBadge :variant="statusVariant(detail.post.graph_status)" text="graph" :dot="false"/>
            <span class="text-xs muted mono">{{ detail.post.graph_status || '—' }}</span>
          </div>
        </dd>
        <dt>Retry count</dt><dd class="mono">{{ detail.post.retry_count != null ? detail.post.retry_count : '—' }}</dd>
        <dt>Теги</dt>
        <dd>
          <div v-if="detail.post.tags && detail.post.tags.length" class="row" style="gap:6px;flex-wrap:wrap">
            <UiBadge v-for="(t, i) in detail.post.tags" :key="i" variant="accent" :text="t" :dot="false"/>
          </div>
          <span v-else class="muted">—</span>
        </dd>
        <dt>Опубликовано</dt><dd>{{ fmtDate(detail.post.published_at) }}</dd>
        <dt>Создано</dt><dd>{{ fmtDate(detail.post.created_at) }}</dd>
        <dt>Ошибка</dt><dd :style="detail.post.error_message ? 'color:#fca5a5' : ''">{{ detail.post.error_message || '—' }}</dd>
      </div>

      <div v-if="detail.post.preview" class="mt-4">
        <h4 class="mb-3">Превью</h4>
        <div class="muted text-sm">{{ detail.post.preview }}</div>
      </div>

      <div v-if="detailLoading" class="state"><div class="spin" style="margin:0 auto 12px"></div><div class="state__msg">Загрузка деталей…</div></div>
      <div v-else-if="detailErr" class="field__error mt-4">Не удалось загрузить детали: {{ detailErr.detail || detailErr.message }}</div>

      <template v-else>
        <h4 class="mt-4 mb-3">Enrichments <span class="faint text-xs">({{ detail.enrichments.length }})</span></h4>
        <div v-if="detail.enrichments.length">
          <div v-for="(e, i) in detail.enrichments" :key="i" class="mt-2">
            <div class="row" style="gap:8px;margin-bottom:6px">
              <UiBadge variant="accent" :text="e.kind" :dot="false"/>
              <span v-if="e.s3_key" class="text-xs faint mono ellip" :title="e.s3_key">{{ e.s3_key }}</span>
            </div>
            <JsonView :data="e.data"/>
          </div>
        </div>
        <div v-else class="muted text-sm">Обогащений нет.</div>

        <template v-if="detail.album">
          <h4 class="mt-4 mb-3">Альбом</h4>
          <JsonView :data="detail.album"/>
        </template>
      </template>
    </UiModal>
  </div>
  `,
  created() { this.statusOptions = STATUS_OPTIONS; },
};
