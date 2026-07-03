import { api } from '../api.js';
import { fmtAgo, fmtDate, fmtNum, statusVariant } from '../format.js';

export default {
  name: 'AlbumsView',
  data() {
    return {
      albums: [], loading: true, error: null,
      filters: {
        workspace: this.$route.query.ws || '',
        source_id: '', assembled: '', q: '', limit: 50,
      },
      detail: null, detailLoading: false, detailErr: null,
    };
  },
  computed: {
    filtered() {
      const q = this.filters.q.trim().toLowerCase();
      if (!q) return this.albums;
      return this.albums.filter((a) => {
        const hay = `${a.grouped_id || ''} ${a.id || ''} ${a.source_id || ''}`.toLowerCase();
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
    fmtAgo, fmtDate, fmtNum, statusVariant,
    normLimit() {
      let n = Number(this.filters.limit) || 50;
      n = Math.max(1, Math.min(200, Math.floor(n)));
      this.filters.limit = n;
      return n;
    },
    async load() {
      this.loading = true; this.error = null;
      try {
        const data = await api.get('/api/albums', {
          workspace_id: this.filters.workspace || undefined,
          source_id: this.filters.source_id.trim() || undefined,
          assembled: this.filters.assembled || undefined,
          limit: this.normLimit(),
        });
        this.albums = Array.isArray(data) ? data : [];
      } catch (e) { this.error = e; }
      finally { this.loading = false; }
    },
    async openDetail(a) {
      this.detail = { album: a, posts: [] };
      this.detailLoading = true; this.detailErr = null;
      try {
        const data = await api.get(`/api/albums/${encodeURIComponent(a.id)}`);
        this.detail = {
          album: (data && data.album) || a,
          posts: (data && data.posts) || [],
        };
      } catch (e) { this.detailErr = e; }
      finally { this.detailLoading = false; }
    },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Альбомы</h1>
        <p>{{ albums.length }} загружено · показано {{ filtered.length }}</p>
      </div>
    </div>

    <div class="toolbar">
      <div class="toolbar__group"><label>Workspace</label><WorkspaceSelect v-model="filters.workspace"/></div>
      <div class="toolbar__group"><label>Источник</label>
        <input class="input mono" v-model="filters.source_id" placeholder="source_id" @change="load">
      </div>
      <div class="toolbar__group"><label>Собран</label>
        <select class="select" v-model="filters.assembled" @change="load">
          <option value="">Любой</option>
          <option value="true">Собранные</option>
          <option value="false">Несобранные</option>
        </select>
      </div>
      <div class="toolbar__group" style="max-width:110px"><label>Лимит</label>
        <input class="input" type="number" min="1" max="200" v-model.number="filters.limit" @change="load">
      </div>
      <div class="toolbar__group" style="flex:1;min-width:180px"><label>Поиск</label>
        <input class="input" v-model="filters.q" placeholder="grouped_id, ID, источник…">
      </div>
      <div class="toolbar__group"><label>&nbsp;</label><button class="btn btn--outline" @click="load" :disabled="loading">↻ Обновить</button></div>
    </div>

    <div class="card"><div class="card__body--flush">
      <StateBlock :loading="loading" :error="error" :empty="!filtered.length"
        :empty-text="albums.length ? 'Нет альбомов по фильтрам' : 'Альбомов пока нет'" @retry="load">
        <div class="table-wrap">
          <table class="tbl tbl--fixed">
            <colgroup>
              <col style="width:26%"><col style="width:18%"><col style="width:10%">
              <col style="width:10%"><col style="width:12%"><col style="width:11%"><col style="width:13%">
            </colgroup>
            <thead><tr>
              <th>Альбом</th><th>Источник</th><th class="num">Элементов</th>
              <th class="num">Постов</th><th>Собран</th><th>Vision</th><th>Создан</th>
            </tr></thead>
            <tbody>
              <tr v-for="a in filtered" :key="a.id" style="cursor:pointer" @click="openDetail(a)">
                <td>
                  <div class="ellip mono" style="font-weight:600" :title="a.grouped_id || a.id">{{ a.grouped_id || a.id }}</div>
                  <div class="text-xs faint mono ellip" :title="a.id">{{ a.id }}</div>
                </td>
                <td class="muted"><span class="ellip" style="display:block" :title="a.source_id">{{ a.source_id || '—' }}</span></td>
                <td class="num mono">{{ fmtNum(a.item_count) }}</td>
                <td class="num mono">{{ fmtNum(a.posts_count) }}</td>
                <td><UiBadge :variant="a.assembled ? 'success' : 'neutral'" :text="a.assembled ? 'собран' : 'нет'"/></td>
                <td>
                  <UiBadge :variant="a.vision_summary_s3_key ? 'success' : 'neutral'"
                    :text="a.vision_summary_s3_key ? 'есть' : 'нет'" :dot="false"/>
                </td>
                <td class="muted text-sm" :title="fmtDate(a.created_at)">{{ a.created_at ? fmtAgo(a.created_at) : '—' }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </StateBlock>
    </div></div>

    <!-- Detail -->
    <UiModal v-if="detail" :title="'Альбом ' + (detail.album.grouped_id || detail.album.id || '')" size="lg" @close="detail = null">
      <div class="dl">
        <dt>ID</dt><dd class="mono">{{ detail.album.id }}</dd>
        <dt>Grouped ID</dt><dd class="mono">{{ detail.album.grouped_id || '—' }}</dd>
        <dt>Workspace</dt><dd>{{ detail.album.workspace_id || '—' }}</dd>
        <dt>Источник</dt><dd class="mono">{{ detail.album.source_id || '—' }}</dd>
        <dt>Элементов</dt><dd class="mono">{{ fmtNum(detail.album.item_count) }}</dd>
        <dt>Постов</dt><dd class="mono">{{ fmtNum(detail.album.posts_count) }}</dd>
        <dt>Собран</dt>
        <dd><UiBadge :variant="detail.album.assembled ? 'success' : 'neutral'" :text="detail.album.assembled ? 'да' : 'нет'"/></dd>
        <dt>Vision summary</dt><dd class="mono ellip" :title="detail.album.vision_summary_s3_key">{{ detail.album.vision_summary_s3_key || '—' }}</dd>
        <dt>Vision labels</dt>
        <dd>
          <div v-if="detail.album.vision_labels && detail.album.vision_labels.length" class="row" style="gap:6px;flex-wrap:wrap">
            <UiBadge v-for="(l, i) in detail.album.vision_labels" :key="i" variant="accent" :text="l" :dot="false"/>
          </div>
          <span v-else class="muted">—</span>
        </dd>
        <dt>Создан</dt><dd>{{ fmtDate(detail.album.created_at) }}</dd>
        <dt>Последний пост</dt><dd>{{ detail.album.last_post_at ? fmtDate(detail.album.last_post_at) : '—' }}</dd>
      </div>

      <h4 class="mt-4 mb-3">Связанные посты <span class="faint text-xs">({{ detail.posts.length }})</span></h4>
      <div v-if="detailLoading" class="state"><div class="spin" style="margin:0 auto 12px"></div><div class="state__msg">Загрузка постов…</div></div>
      <div v-else-if="detailErr" class="field__error">Не удалось загрузить: {{ detailErr.detail || detailErr.message }}</div>
      <div v-else-if="detail.posts.length" class="table-wrap">
        <table class="tbl tbl--fixed" style="min-width:520px">
          <colgroup><col style="width:34%"><col style="width:36%"><col style="width:30%"></colgroup>
          <thead><tr><th>ID</th><th>Превью</th><th>Опубликовано</th></tr></thead>
          <tbody>
            <tr v-for="p in detail.posts" :key="p.id">
              <td class="mono ellip" :title="p.id">{{ p.id }}</td>
              <td class="muted ellip" :title="p.preview || p.content">{{ p.preview || p.content || '—' }}</td>
              <td class="muted text-sm">{{ p.published_at || p.created_at ? fmtAgo(p.published_at || p.created_at) : '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div v-else class="muted text-sm">Связанных постов нет.</div>
    </UiModal>
  </div>
  `,
};
