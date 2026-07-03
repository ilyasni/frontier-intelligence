// Карточка секции настроек: заголовок + описание + статус-бейдж в шапке, тело — слот.
// Оборачивает StateBlock, чтобы падение/загрузка одной секции не ломали остальные.
export default {
  name: 'SettingCard',
  props: {
    title: { type: String, required: true },
    desc: { type: String, default: '' },
    badgeText: { type: [String, Number], default: '' },
    badgeVariant: { type: String, default: 'neutral' },
    loading: Boolean,
    error: { type: [String, Object], default: null },
    empty: Boolean,
    emptyText: { type: String, default: 'Нет данных' },
  },
  emits: ['retry'],
  template: `
    <div class="card">
      <div class="card__head">
        <div style="flex:1;min-width:0">
          <h2>{{ title }}</h2>
          <div v-if="desc" class="sub">{{ desc }}</div>
        </div>
        <UiBadge v-if="badgeText !== '' && badgeText != null" :variant="badgeVariant" :text="badgeText" :dot="false"/>
        <slot name="head-actions"/>
      </div>
      <div class="card__body">
        <StateBlock :loading="loading" :error="error" :empty="empty" :empty-text="emptyText" @retry="$emit('retry')">
          <slot/>
        </StateBlock>
      </div>
    </div>
  `,
};
