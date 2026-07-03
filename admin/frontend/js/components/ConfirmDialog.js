import { confirmState, resolveConfirm } from '../ui.js';

export default {
  name: 'ConfirmDialog',
  computed: { s() { return confirmState; } },
  methods: {
    yes() { resolveConfirm(true); },
    no() { resolveConfirm(false); },
  },
  mounted() {
    this._esc = (e) => { if (e.key === 'Escape' && confirmState.open) this.no(); };
    document.addEventListener('keydown', this._esc);
  },
  unmounted() { document.removeEventListener('keydown', this._esc); },
  template: `
    <teleport to="body">
      <div v-if="s.open" class="modal-backdrop" @mousedown.self="no">
        <div class="modal" style="max-width:440px" role="alertdialog" aria-modal="true">
          <div class="modal__head"><h3>{{ s.title }}</h3></div>
          <div class="modal__body"><p style="margin:0;color:var(--text-muted)">{{ s.message }}</p></div>
          <div class="modal__foot">
            <button class="btn btn--outline" @click="no">Отмена</button>
            <button class="btn" :class="s.danger ? 'btn--danger' : 'btn--primary'" @click="yes">{{ s.confirmText }}</button>
          </div>
        </div>
      </div>
    </teleport>
  `,
};
