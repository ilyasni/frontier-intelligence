// Единый блок состояний загрузки/ошибки/пусто. Оборачивает контент:
// <StateBlock :loading="loading" :error="error" :empty="!rows.length" @retry="load">...</StateBlock>
export default {
  name: 'StateBlock',
  props: {
    loading: Boolean,
    error: { type: [String, Object], default: null },
    empty: Boolean,
    emptyText: { type: String, default: 'Ничего не найдено' },
    emptyIcon: { type: String, default: '∅' },
  },
  emits: ['retry'],
  computed: {
    errorText() {
      if (!this.error) return '';
      return typeof this.error === 'string' ? this.error : (this.error.message || 'Неизвестная ошибка');
    },
  },
  template: `
    <div v-if="loading" class="state"><div class="spin" style="margin:0 auto 12px"></div><div class="state__msg">Загрузка…</div></div>
    <div v-else-if="error" class="state state--error">
      <div class="state__icon">⚠</div>
      <div class="state__title">Ошибка загрузки</div>
      <div class="state__msg">{{ errorText }}</div>
      <button class="btn btn--outline btn--sm mt-4" @click="$emit('retry')">Повторить</button>
    </div>
    <div v-else-if="empty" class="state"><div class="state__icon">{{ emptyIcon }}</div><div class="state__msg">{{ emptyText }}</div></div>
    <slot v-else/>
  `,
};
