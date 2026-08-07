import { api } from '../api.js';
import { fmtAgo, fmtDate, fmtBytes, fmtNum } from '../format.js';

export default {
  name: 'MediaView',
  data() {
    return {
      media: [], loading: true, error: null,
      filters: {
        workspace: this.$route.query.ws || '',
        mime_type: '', q: '', limit: 50,
      },
      detail: null, detailLoading: false, detailErr: null,
    };
  },
  computed: {
    filtered() {
      const q = this.filters.q.trim().toLowerCase();
      if (!q) return this.media;
      return this.media.filter((m) => {
        const hay = `${m.sha256 || ''} ${m.s3_key || ''} ${m.mime_type || ''}`.toLowerCase();
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
    fmtAgo, fmtDate, fmtBytes, fmtNum,
    normLimit() {
      let n = Number(this.filters.limit) || 50;
      n = Math.max(1, Math.min(200, Math.floor(n)));
      this.filters.limit = n;
      return n;
    },
    async load() {
      this.loading = true; this.error = null;
      try {
        const data = await api.get('/api/media', {
          workspace_id: this.filters.workspace || undefined,
          mime_type: this.filters.mime_type.trim() || undefined,
          limit: this.normLimit(),
        });
        this.media = Array.isArray(data) ? data : [];
      } catch (e) { this.error = e; }
      finally { this.loading = false; }
    },
    async openDetail(m) {
      this.detail = { media: m, posts: [], vision: [] };
      this.detailLoading = true; this.detailErr = null;
      try {
        const data = await api.get(`/api/media/${encodeURIComponent(m.sha256)}`);
        this.detail = {
          media: (data && data.media) || m,
          posts: (data && data.posts) || [],
          vision: (data && data.vision) || [],
        };
      } catch (e) { this.detailErr = e; }
      finally { this.detailLoading = false; }
    },
    shortSha(s) { return s ? String(s).slice(0, 16) : '—'; },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Медиа</h1>
        <p>{{ media.length }} загружено · показано {{ filtered.length }}</p>
      </div>
    </div>

    <div class="toolbar">
      <div class="toolbar__group"><label>Workspace</label><WorkspaceSelect v-model="filters.workspace"/></div>
      <div class="toolbar__group"><label>MIME-тип</label>
        <input class="input mono" v-model="filters.mime_type" placeholder="image/jpeg" @change="load">
      </div>
      <div class="toolbar__group" style="max-width:110px"><label>Лимит</label>
        <input class="input" type="number" min="1" max="200" v-model.number="filters.limit" @change="load">
      </div>
      <div class="toolbar__group" style="flex:1;min-width:200px"><label>Поиск</label>
        <input class="input" v-model="filters.q" placeholder="sha256, s3_key, MIME…">
      </div>
      <div class="toolbar__group"><label>&nbsp;</label><button class="btn btn--outline" @click="load" :disabled="loading">↻ Обновить</button></div>
    </div>

    <div class="card"><div class="card__body--flush">
      <StateBlock :loading="loading" :error="error" :empty="!filtered.length"
        :empty-text="media.length ? 'Нет медиа по фильтрам' : 'Медиа пока нет'" @retry="load">
        <div class="table-wrap">
          <table class="tbl tbl--fixed">
            <colgroup>
              <col style="width:34%"><col style="width:16%"><col style="width:12%">
              <col style="width:11%"><col style="width:15%"><col style="width:12%">
            </colgroup>
            <thead><tr>
              <th>Медиа</th><th>MIME</th><th class="num">Размер</th>
              <th class="num">Постов</th><th>Workspace</th><th>Создано</th>
            </tr></thead>
            <tbody>
              <tr v-for="m in filtered" :key="m.sha256" style="cursor:pointer" tabindex="0" @click="openDetail(m)" @keydown.enter.prevent="openDetail(m)" @keydown.space.prevent="openDetail(m)">
                <td>
                  <div class="ellip mono" style="font-weight:600" :title="m.sha256">{{ shortSha(m.sha256) }}</div>
                  <div class="text-xs faint mono ellip" :title="m.s3_key">{{ m.s3_key || '—' }}</div>
                </td>
                <td class="muted"><span class="ellip" style="display:block" :title="m.mime_type">{{ m.mime_type || '—' }}</span></td>
                <td class="num mono">{{ fmtBytes(m.size_bytes) }}</td>
                <td class="num mono">{{ fmtNum(m.posts_count) }}</td>
                <td class="muted"><span class="ellip" style="display:block" :title="m.workspace_id">{{ m.workspace_id || '—' }}</span></td>
                <td class="muted text-sm" :title="fmtDate(m.created_at)">{{ m.created_at ? fmtAgo(m.created_at) : '—' }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </StateBlock>
    </div></div>

    <!-- Detail -->
    <UiModal v-if="detail" :title="'Медиа ' + shortSha(detail.media.sha256)" size="lg" @close="detail = null">
      <div class="dl">
        <dt>SHA-256</dt><dd class="mono" style="word-break:break-all">{{ detail.media.sha256 }}</dd>
        <dt>S3 key</dt><dd class="mono" style="word-break:break-all">{{ detail.media.s3_key || '—' }}</dd>
        <dt>MIME-тип</dt><dd class="mono">{{ detail.media.mime_type || '—' }}</dd>
        <dt>Размер</dt><dd class="mono">{{ fmtBytes(detail.media.size_bytes) }}</dd>
        <dt>Workspace</dt><dd>{{ detail.media.workspace_id || '—' }}</dd>
        <dt>Постов</dt><dd class="mono">{{ fmtNum(detail.media.posts_count) }}</dd>
        <dt>Создано</dt><dd>{{ fmtDate(detail.media.created_at) }}</dd>
      </div>

      <div v-if="detailLoading" class="state"><div class="spin" style="margin:0 auto 12px"></div><div class="state__msg">Загрузка деталей…</div></div>
      <div v-else-if="detailErr" class="field__error mt-4">Не удалось загрузить детали: {{ detailErr.detail || detailErr.message }}</div>

      <template v-else>
        <h4 class="mt-4 mb-3">Связанные посты <span class="faint text-xs">({{ detail.posts.length }})</span></h4>
        <div v-if="detail.posts.length" class="table-wrap">
          <table class="tbl tbl--fixed" style="min-width:520px">
            <colgroup><col style="width:34%"><col style="width:24%"><col style="width:42%"></colgroup>
            <thead><tr><th>ID</th><th>Источник</th><th>Превью</th></tr></thead>
            <tbody>
              <tr v-for="p in detail.posts" :key="p.id">
                <td class="mono ellip" :title="p.id">{{ p.id }}</td>
                <td class="muted mono ellip" :title="p.source_id">{{ p.source_id || '—' }}</td>
                <td class="muted ellip" :title="p.preview || p.content">{{ p.preview || p.content || '—' }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-else class="muted text-sm">Связанных постов нет.</div>

        <h4 class="mt-4 mb-3">Vision <span class="faint text-xs">({{ detail.vision.length }})</span></h4>
        <div v-if="detail.vision.length">
          <div v-for="(v, i) in detail.vision" :key="i" class="mt-2">
            <div v-if="v.post_id" class="text-xs faint mono" style="margin-bottom:6px">{{ v.post_id }}</div>
            <JsonView :data="v.data != null ? v.data : v"/>
          </div>
        </div>
        <div v-else class="muted text-sm">Vision-данных нет.</div>
      </template>
    </UiModal>
  </div>
  `,
};
