import { login } from '../auth.js';

export default {
  name: 'LoginView',
  emits: ['success'],
  data() { return { username: 'admin', password: '', err: '', busy: false }; },
  mounted() {
    this.$nextTick(() => { const el = this.$refs.pw; if (el) el.focus(); });
  },
  methods: {
    async submit() {
      if (this.busy) return;
      this.err = ''; this.busy = true;
      try {
        await login(this.username.trim(), this.password);
        this.$emit('success');
      } catch (e) {
        this.err = e.detail || 'Ошибка входа';
      } finally {
        this.busy = false;
      }
    },
  },
  template: `
    <div style="min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px">
      <form class="card" style="width:100%;max-width:360px" @submit.prevent="submit">
        <div class="card__body">
          <div class="row" style="gap:10px;margin-bottom:20px">
            <span class="dot" style="width:12px;height:12px;border-radius:999px;background:var(--accent);box-shadow:0 0 0 5px var(--accent-soft)"></span>
            <div><div style="font-weight:700;font-size:16px">Frontier Admin</div><div class="text-xs faint">Вход в панель управления</div></div>
          </div>
          <div class="field">
            <label class="field__label">Логин</label>
            <input class="input" v-model="username" autocomplete="username">
          </div>
          <div class="field">
            <label class="field__label">Пароль</label>
            <input class="input" type="password" v-model="password" ref="pw" autocomplete="current-password">
          </div>
          <div v-if="err" class="field__error" style="margin-bottom:12px">{{ err }}</div>
          <button class="btn btn--primary" type="submit" style="width:100%" :disabled="busy">
            <span v-if="busy" class="spin"></span> Войти
          </button>
        </div>
      </form>
    </div>
  `,
};
