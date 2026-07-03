// Доступная модалка: role=dialog, закрытие по Esc и клику на фон, автофокус, блок скролла фона.
// Слоты: default (тело), footer (кнопки).
export default {
  name: 'UiModal',
  props: {
    title: { type: String, default: '' },
    size: { type: String, default: '' }, // '' | 'lg'
  },
  emits: ['close'],
  mounted() {
    this._esc = (e) => { if (e.key === 'Escape') this.$emit('close'); };
    document.addEventListener('keydown', this._esc);
    document.body.style.overflow = 'hidden';
    this.$nextTick(() => {
      const el = this.$refs.modal && this.$refs.modal.querySelector('input, select, textarea');
      if (el && el.focus) el.focus();
    });
  },
  unmounted() {
    document.removeEventListener('keydown', this._esc);
    document.body.style.overflow = '';
  },
  template: `
    <teleport to="body">
      <div class="modal-backdrop" @mousedown.self="$emit('close')">
        <div class="modal" :class="size === 'lg' && 'modal--lg'" role="dialog" aria-modal="true" ref="modal">
          <div class="modal__head">
            <h3>{{ title }}</h3>
            <button class="btn btn--icon btn--ghost" @click="$emit('close')" aria-label="Закрыть">✕</button>
          </div>
          <div class="modal__body"><slot/></div>
          <div class="modal__foot" v-if="$slots.footer"><slot name="footer"/></div>
        </div>
      </div>
    </teleport>
  `,
};
