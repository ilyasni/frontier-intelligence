import { api } from '../api.js';
import { truncate } from '../format.js';

// Поиск сигналов: frontier (POST /api/search) и balanced (POST /api/search/balanced).
// Ответ приходит как отдаёт MCP. Всё выводим через {{ }} / :attr — экранирование за нас
// делает Vue; сырые объекты — через <JsonView>, а не pre на весь ответ (в старом фронте
// там была XSS-дыра на innerHTML).
const LANGS = [
  { v: '', l: 'все' },
  { v: 'ru', l: 'ru' },
  { v: 'en', l: 'en' },
];
const REGIONS = [
  { v: '', l: 'все' },
  { v: 'ru', l: 'ru' },
  { v: 'global', l: 'global' },
  { v: 'us', l: 'us' },
  { v: 'eu', l: 'eu' },
];

// Безопасная ссылка: рендерим <a> только для http/https, иначе — просто текст.
function safeUrl(u) {
  if (!u || typeof u !== 'string') return null;
  return /^https?:\/\//i.test(u.trim()) ? u.trim() : null;
}

export default {
  name: 'SearchView',
  data() {
    return {
      form: {
        query: '',
        mode: 'frontier', // 'frontier' | 'balanced'
        workspace: this.$route.query.ws || '',
        limit: 10,
        lang: '',
        days: 7,
        region: '',
        entities: '',
      },
      busy: false,
      error: null,
      searched: false,
      result: null,
    };
  },
  computed: {
    langs() { return LANGS; },
    regions() { return REGIONS; },
    isBalanced() { return this.form.mode === 'balanced'; },
    // frontier: плоский список результатов
    frontierResults() {
      const r = this.result;
      return (r && Array.isArray(r.results)) ? r.results : [];
    },
    // balanced: несколько «дорожек»
    lanes() {
      const r = this.result || {};
      return [
        { title: 'Сигналы', items: Array.isArray(r.signals) ? r.signals : [] },
        { title: 'Контр-сигналы', items: Array.isArray(r.counter_signals) ? r.counter_signals : [] },
        { title: 'RU-верификация', items: (r.ru_verification && Array.isArray(r.ru_verification.results)) ? r.ru_verification.results : [] },
      ];
    },
    synthesis() {
      const s = this.result && this.result.synthesis;
      if (!s) return null;
      return s.parsed || s.raw || s;
    },
    blindSpots() {
      const b = this.result && (this.result.known_blind_spots || this.result.blind_spots);
      return (Array.isArray(b) && b.length) ? b : null;
    },
    competitors() {
      const c = this.result && this.result.competitors;
      if (c == null) return null;
      if (Array.isArray(c)) return c.length ? c : null;
      return c;
    },
    // Пусто, если ни в одной секции нет данных.
    isEmpty() {
      if (!this.result) return true;
      if (this.isBalanced) {
        return !this.lanes.some((l) => l.items.length) && !this.synthesis && !this.blindSpots && !this.competitors;
      }
      return !this.frontierResults.length;
    },
  },
  watch: {
    'form.workspace'(v) {
      this.$router.replace({ query: { ...this.$route.query, ws: v || undefined } });
    },
  },
  methods: {
    truncate, safeUrl,
    // Нормализуем одну запись результата к единому виду для карточки.
    normItem(r, i) {
      const p = r.payload || {};
      const concepts = (Array.isArray(p.concepts) ? p.concepts : (Array.isArray(r.concepts) ? r.concepts : [])).slice(0, 8);
      return {
        idx: i + 1,
        score: typeof r.score === 'number' ? r.score.toFixed(3) : null,
        content: r.content || p.content || '',
        author: p.author || r.author || '',
        url: safeUrl(p.url || r.url),
        category: p.category || r.category || p.signal_type || '',
        region: p.source_region || '',
        sourceScore: p.source_score != null ? Number(p.source_score).toFixed(2) : null,
        concepts,
      };
    },
    async runSearch() {
      const q = this.form.query.trim();
      if (!q) { this.error = 'Введите запрос'; return; }
      this.busy = true; this.error = null; this.searched = true; this.result = null;
      const f = this.form;
      const body = {
        query: q,
        workspace: f.workspace || undefined,
        limit: Number(f.limit) || 10,
      };
      if (f.lang) body.lang = f.lang;
      if (f.days) body.days = Number(f.days) || undefined;
      if (f.region) body.region = f.region;
      const entities = f.entities.split(',').map((v) => v.trim()).filter(Boolean);
      if (entities.length) body.entities = entities;
      if (this.isBalanced) body.synthesize = true;

      const endpoint = this.isBalanced ? '/api/search/balanced' : '/api/search';
      try {
        this.result = await api.post(endpoint, body);
      } catch (e) { this.error = e; }
      finally { this.busy = false; }
    },
  },
  template: `
  <div>
    <div class="page-head">
      <div class="page-head__text">
        <h1>Поиск сигналов</h1>
        <p>Frontier — быстрый поиск по индексу · Balanced — синтез с контр-сигналами и RU-верификацией</p>
      </div>
    </div>

    <div class="card"><div class="card__body">
      <div class="field">
        <label class="field__label">Запрос <span class="req">*</span></label>
        <input class="input" v-model="form.query" placeholder="Что ищем на фронтире?" @keydown.enter="runSearch">
      </div>

      <div class="field-row">
        <div class="field" style="max-width:180px">
          <label class="field__label">Режим</label>
          <select class="select" v-model="form.mode">
            <option value="frontier">Frontier</option>
            <option value="balanced">Balanced</option>
          </select>
        </div>
        <div class="field">
          <label class="field__label">Workspace</label>
          <WorkspaceSelect v-model="form.workspace"/>
        </div>
        <div class="field" style="max-width:110px">
          <label class="field__label">Лимит</label>
          <input class="input" type="number" min="1" max="100" v-model.number="form.limit">
        </div>
        <div class="field" style="max-width:120px">
          <label class="field__label">Язык</label>
          <select class="select" v-model="form.lang"><option v-for="o in langs" :key="o.v" :value="o.v">{{ o.l }}</option></select>
        </div>
        <div class="field" style="max-width:110px">
          <label class="field__label">Дней</label>
          <input class="input" type="number" min="1" max="365" v-model.number="form.days">
        </div>
        <div class="field" style="max-width:140px">
          <label class="field__label">Регион</label>
          <select class="select" v-model="form.region"><option v-for="o in regions" :key="o.v" :value="o.v">{{ o.l }}</option></select>
        </div>
      </div>

      <div class="field">
        <label class="field__label">Сущности / конкуренты</label>
        <input class="input" v-model="form.entities" placeholder="Tesla, BYD, GM (через запятую)">
      </div>

      <div class="row">
        <button class="btn btn--primary" :disabled="busy" @click="runSearch">
          <span v-if="busy" class="spin"></span> Поиск
        </button>
        <span v-if="busy" class="muted text-sm">Ищем сигналы…</span>
      </div>
    </div></div>

    <div style="margin-top:var(--sp-4)">
      <StateBlock :loading="busy" :error="error"
        :empty="searched && isEmpty"
        empty-text="Ничего не найдено — добавьте источники и дайте пайплайну проиндексировать посты"
        @retry="runSearch">

        <template v-if="searched && result">

          <!-- Balanced: синтез -->
          <div v-if="isBalanced && synthesis" class="card" style="border-color:var(--accent-border)">
            <div class="card__head"><h2>Синтез (balanced)</h2></div>
            <div class="card__body">
              <p v-if="typeof synthesis === 'string'" style="margin:0;white-space:pre-wrap">{{ synthesis }}</p>
              <JsonView v-else :data="synthesis"/>
            </div>
          </div>

          <!-- Balanced: слепые зоны -->
          <div v-if="isBalanced && blindSpots" class="card">
            <div class="card__head"><h2>Известные слепые зоны</h2><span class="sub">{{ blindSpots.length }}</span></div>
            <div class="card__body">
              <ul style="margin:0;padding-left:20px">
                <li v-for="(b, i) in blindSpots" :key="i" style="margin-bottom:6px">
                  <span v-if="typeof b === 'string'">{{ b }}</span>
                  <JsonView v-else :data="b"/>
                </li>
              </ul>
            </div>
          </div>

          <!-- Balanced: дорожки -->
          <template v-if="isBalanced">
            <div v-for="lane in lanes" :key="lane.title" class="card">
              <div class="card__head"><h2>{{ lane.title }}</h2><span class="sub">{{ lane.items.length }}</span></div>
              <div class="card__body">
                <div v-if="!lane.items.length" class="muted text-sm">Нет данных</div>
                <div v-for="(raw, i) in lane.items" :key="i" class="stack"
                  style="padding:var(--sp-3) 0;border-bottom:1px solid var(--border)">
                  <div class="row row--wrap" style="gap:8px">
                    <UiBadge variant="accent" :text="'#' + normItem(raw, i).idx" :dot="false"/>
                    <span v-if="normItem(raw, i).score" class="text-xs mono muted">score {{ normItem(raw, i).score }}</span>
                    <span v-if="normItem(raw, i).category" class="text-xs faint">{{ normItem(raw, i).category }}</span>
                    <span v-if="normItem(raw, i).region" class="text-xs faint">· {{ normItem(raw, i).region }}</span>
                  </div>
                  <div class="text-sm" style="white-space:pre-wrap">{{ truncate(normItem(raw, i).content, 600) }}</div>
                  <div class="row row--wrap text-xs muted" style="gap:8px">
                    <span v-if="normItem(raw, i).author">👤 {{ normItem(raw, i).author }}</span>
                    <a v-if="normItem(raw, i).url" :href="normItem(raw, i).url" target="_blank" rel="noopener noreferrer">🔗 ссылка</a>
                    <span v-if="normItem(raw, i).concepts.length">· {{ normItem(raw, i).concepts.join(', ') }}</span>
                  </div>
                </div>
              </div>
            </div>

            <!-- Balanced: конкуренты -->
            <div v-if="competitors" class="card">
              <div class="card__head"><h2>Конкуренты</h2></div>
              <div class="card__body"><JsonView :data="competitors"/></div>
            </div>
          </template>

          <!-- Frontier: плоский список -->
          <div v-else class="card">
            <div class="card__head"><h2>Результаты</h2><span class="sub">{{ frontierResults.length }}</span></div>
            <div class="card__body">
              <div v-for="(raw, i) in frontierResults" :key="i" class="stack"
                style="padding:var(--sp-3) 0;border-bottom:1px solid var(--border)">
                <div class="row row--wrap" style="gap:8px">
                  <UiBadge variant="accent" :text="'#' + normItem(raw, i).idx" :dot="false"/>
                  <span v-if="normItem(raw, i).score" class="text-xs mono muted">score {{ normItem(raw, i).score }}</span>
                  <span v-if="normItem(raw, i).category" class="text-xs faint">{{ normItem(raw, i).category }}</span>
                  <span v-if="normItem(raw, i).sourceScore" class="text-xs faint">· src {{ normItem(raw, i).sourceScore }}</span>
                </div>
                <div class="text-sm" style="white-space:pre-wrap">{{ truncate(normItem(raw, i).content, 600) }}</div>
                <div class="row row--wrap text-xs muted" style="gap:8px">
                  <span v-if="normItem(raw, i).author">👤 {{ normItem(raw, i).author }}</span>
                  <a v-if="normItem(raw, i).url" :href="normItem(raw, i).url" target="_blank" rel="noopener noreferrer">🔗 ссылка</a>
                  <span v-if="normItem(raw, i).concepts.length">· {{ normItem(raw, i).concepts.join(', ') }}</span>
                </div>
              </div>
            </div>
          </div>

        </template>
      </StateBlock>
    </div>
  </div>
  `,
};
