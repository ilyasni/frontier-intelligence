import { api } from '../api.js';
import { fmtNum } from '../format.js';

// Единственный экземпляр библиотеки Cytoscape — вендорится локально в /static/vendor,
// в index.html её нет. Грузим динамически один раз, промис кешируем на модуле.
const CYTOSCAPE_SRC = '/static/vendor/cytoscape.min.js';
let cytoscapeLoader = null;
function loadCytoscape() {
  if (window.cytoscape) return Promise.resolve(window.cytoscape);
  if (cytoscapeLoader) return cytoscapeLoader;
  cytoscapeLoader = new Promise((resolve, reject) => {
    const el = document.createElement('script');
    el.src = CYTOSCAPE_SRC;
    el.async = true;
    el.onload = () => (window.cytoscape ? resolve(window.cytoscape) : reject(new Error('Cytoscape не инициализировался')));
    el.onerror = () => {
      cytoscapeLoader = null; // дать повторить попытку
      reject(new Error('Не удалось загрузить библиотеку графа'));
    };
    document.head.appendChild(el);
  });
  return cytoscapeLoader;
}

// Цвет узла/ребра по категории — из дизайн-токенов (var(--...)), а не hex.
const CATEGORY_COLORS = [
  { match: 'tech', color: 'var(--info)' },
  { match: 'design', color: 'var(--warn)' },
  { match: 'business', color: 'var(--success)' },
];
function categoryColor(category) {
  const v = String(category || '').toLowerCase();
  for (const c of CATEGORY_COLORS) if (v.includes(c.match)) return c.color;
  return 'var(--accent)';
}

// Читаем реальное значение CSS-переменной — Cytoscape рисует на canvas и var(--...) не понимает.
function cssVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}
function resolveColor(token) {
  const m = /^var\((--[\w-]+)\)$/.exec(token);
  return m ? cssVar(m[1], '#6366f1') : token;
}

export default {
  name: 'GraphView',
  data() {
    return {
      loading: false, error: null, built: false,
      filters: {
        workspace: this.$route.query.ws || '',
        limit: 120, min_mentions: 2, max_nodes: 50,
      },
      meta: null,
      detail: null, // { kind:'node'|'edge', data:{...} }
    };
  },
  watch: {
    'filters.workspace'(v) {
      this.$router.replace({ query: { ...this.$route.query, ws: v || undefined } });
    },
  },
  mounted() {
    this.load();
  },
  unmounted() {
    this.destroyGraph();
  },
  methods: {
    fmtNum,
    categoryColor,

    destroyGraph() {
      if (this.cy) {
        try { this.cy.destroy(); } catch { /* инстанс уже мёртв — ок */ }
        this.cy = null;
      }
    },

    async load() {
      this.loading = true; this.error = null; this.detail = null;
      try {
        const cy = await loadCytoscape();
        const data = await api.get('/api/graph', {
          workspace_id: this.filters.workspace || undefined,
          limit: Math.min(Number(this.filters.limit) || 120, 500),
          min_mentions: Math.max(Number(this.filters.min_mentions) || 1, 1),
          max_nodes: Math.min(Number(this.filters.max_nodes) || 50, 150),
        });
        this.meta = data && data.meta ? data.meta : null;
        const nodes = (data && data.nodes) || [];
        this.built = nodes.length > 0;
        // Гасим оверлей загрузки ДО построения: контейнер графа всегда в DOM,
        // но cytoscape должен инициализироваться в полностью видимом контейнере.
        this.loading = false;
        // Перед перестроением — уничтожаем прошлый инстанс, чтобы не течь.
        this.destroyGraph();
        if (nodes.length) {
          await this.$nextTick();
          this.buildGraph(cy, data);
        }
      } catch (e) {
        this.error = e;
        this.built = false;
        this.destroyGraph();
      } finally {
        this.loading = false;
      }
    },

    buildGraph(cytoscape, data) {
      const container = this.$refs.canvas;
      if (!container) return;

      const nodes = (data.nodes || []).map((n) => ({
        data: {
          id: n.id,
          label: n.label,
          category: n.category || 'other',
          mentions: n.mentions || 0,
          total_weight: n.total_weight || 0,
          max_weight: n.max_weight || 0,
          avg_weight: n.avg_weight || 0,
          posts_count: n.posts_count || 0,
          posts: n.posts || [],
          color: resolveColor(categoryColor(n.category)),
          size: 22 + Math.min((n.mentions || 0) * 5, 36), // масштаб узла по mentions
        },
      }));
      const edges = (data.edges || []).map((e) => ({
        data: {
          id: e.id,
          source: e.source,
          target: e.target,
          mentions: e.mentions || 0,
          weight: e.weight || 0,
          category: e.category || 'other',
          color: resolveColor(categoryColor(e.category)),
          width: 1 + Math.min((e.weight || 0) * 4, 6), // толщина ребра по weight
        },
      }));

      const cy = cytoscape({
        container,
        elements: [...nodes, ...edges],
        style: [
          {
            selector: 'node',
            style: {
              'background-color': 'data(color)',
              label: 'data(label)',
              color: resolveColor('var(--text)'),
              'font-size': 11,
              'text-wrap': 'wrap',
              'text-max-width': 110,
              'text-valign': 'center',
              'text-halign': 'center',
              width: 'data(size)',
              height: 'data(size)',
              'border-width': 2,
              'border-color': resolveColor('var(--bg)'),
            },
          },
          {
            selector: 'edge',
            style: {
              'line-color': 'data(color)',
              width: 'data(width)',
              'curve-style': 'bezier',
              opacity: 0.65,
            },
          },
          {
            selector: ':selected',
            style: {
              'border-color': resolveColor('var(--text)'),
              'border-width': 3,
              'line-color': resolveColor('var(--text)'),
              opacity: 1,
            },
          },
        ],
        layout: {
          name: 'cose',
          // classic cose с animate:false выполняет ВСЕ итерации синхронно и блокирует
          // главный поток (на ~100 узлах вкладка виснет). Поэтому крупный граф строим
          // АСИНХРОННО (animate:true — не блокирует поток) с урезанным числом итераций,
          // а маленький — синхронно и мгновенно. Подтверждено докой cytoscape.
          animate: nodes.length > 60,
          animationDuration: 400,
          refresh: nodes.length > 60 ? 4 : 10,
          numIter: nodes.length > 60 ? 180 : 1000,
          fit: true,
          padding: 28,
          idealEdgeLength: 110,
          nodeRepulsion: 400000,
          randomize: false,
        },
      });

      cy.on('tap', 'node', (evt) => { this.detail = { kind: 'node', data: evt.target.data() }; });
      cy.on('tap', 'edge', (evt) => { this.detail = { kind: 'edge', data: evt.target.data() }; });
      cy.on('tap', (evt) => { if (evt.target === cy) this.detail = null; }); // тап по фону — закрыть

      this.cy = cy;
    },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Граф концептов</h1>
        <p v-if="meta">
          {{ fmtNum(meta.node_count != null ? meta.node_count : (meta.nodes != null ? meta.nodes : 0)) }} узлов ·
          {{ fmtNum(meta.edge_count != null ? meta.edge_count : (meta.edges != null ? meta.edges : 0)) }} рёбер
        </p>
        <p v-else class="muted">Совстречаемость концептов по постам workspace</p>
      </div>
    </div>

    <div class="toolbar">
      <div class="toolbar__group"><label>Workspace</label><WorkspaceSelect v-model="filters.workspace"/></div>
      <div class="toolbar__group"><label>Recent limit</label>
        <input class="input mono" type="number" min="10" max="500" v-model.number="filters.limit" style="width:110px">
      </div>
      <div class="toolbar__group"><label>Min mentions</label>
        <input class="input mono" type="number" min="1" max="50" v-model.number="filters.min_mentions" style="width:100px">
      </div>
      <div class="toolbar__group"><label>Max nodes</label>
        <input class="input mono" type="number" min="10" max="150" v-model.number="filters.max_nodes" style="width:100px">
      </div>
      <div class="toolbar__group"><label>&nbsp;</label>
        <button class="btn btn--primary" @click="load" :disabled="loading">
          <span v-if="loading" class="spin"></span> Построить
        </button>
      </div>
    </div>

    <div class="card"><div class="card__body--flush" style="position:relative">
      <!-- Контейнер графа ВСЕГДА смонтирован: ref стабилен, cytoscape меряет реальный размер.
           Загрузка/ошибка/пусто — оверлеем поверх, а не подменой слота (иначе ref пропадает
           в момент buildGraph и cytoscape строит в пустоту). -->
      <div ref="canvas" style="width:100%;height:600px;background:var(--bg);border-radius:var(--r-md)"></div>
      <div v-if="loading || error || !built"
        style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;background:var(--bg);border-radius:var(--r-md)">
        <StateBlock :loading="loading" :error="error" :empty="!built"
          empty-text="Нет данных графа для этих фильтров" @retry="load"/>
      </div>
    </div></div>

    <p class="muted text-sm mt-4" v-if="built">Нажми на узел или ребро — откроются детали.</p>

    <!-- Детали узла / ребра -->
    <UiModal v-if="detail" :title="detail.kind === 'node' ? (detail.data.label || detail.data.id) : 'Связь концептов'"
      size="lg" @close="detail = null">
      <template v-if="detail.kind === 'node'">
        <div class="dl">
          <dt>ID</dt><dd class="mono">{{ detail.data.id }}</dd>
          <dt>Категория</dt><dd><UiBadge variant="accent" :text="detail.data.category" :dot="false"/></dd>
          <dt>Упоминаний</dt><dd class="mono">{{ fmtNum(detail.data.mentions) }}</dd>
          <dt>Постов</dt><dd class="mono">{{ fmtNum(detail.data.posts_count) }}</dd>
          <dt>Вес (сумма)</dt><dd class="mono">{{ detail.data.total_weight }}</dd>
          <dt>Вес (макс)</dt><dd class="mono">{{ detail.data.max_weight }}</dd>
          <dt>Вес (средн.)</dt><dd class="mono">{{ Number(detail.data.avg_weight).toFixed(3) }}</dd>
        </div>
        <h4 class="mt-4 mb-3">Примеры постов</h4>
        <div v-if="detail.data.posts && detail.data.posts.length">
          <div v-for="(p, i) in detail.data.posts" :key="i" class="row"
            style="gap:8px;flex-wrap:wrap;padding:8px 0;border-bottom:1px solid var(--border)">
            <span class="mono text-xs">{{ p.post_id }}</span>
            <span class="muted text-xs">{{ p.source_id || '—' }}</span>
            <UiBadge v-if="p.category" variant="neutral" :text="p.category" :dot="false"/>
            <span class="faint text-xs mono" v-if="p.weight != null">weight: {{ p.weight }}</span>
          </div>
        </div>
        <div v-else class="muted text-sm">Нет примеров постов</div>
      </template>

      <template v-else>
        <div class="dl">
          <dt>ID</dt><dd class="mono">{{ detail.data.id }}</dd>
          <dt>От</dt><dd class="mono">{{ detail.data.source }}</dd>
          <dt>К</dt><dd class="mono">{{ detail.data.target }}</dd>
          <dt>Категория</dt><dd><UiBadge variant="neutral" :text="detail.data.category" :dot="false"/></dd>
          <dt>Совстречаемость</dt><dd class="mono">{{ fmtNum(detail.data.mentions) }}</dd>
          <dt>Вес</dt><dd class="mono">{{ detail.data.weight }}</dd>
        </div>
      </template>

      <template v-if="meta">
        <h4 class="mt-4 mb-3">Мета графа</h4>
        <JsonView :data="meta"/>
      </template>
    </UiModal>
  </div>
  `,
};
