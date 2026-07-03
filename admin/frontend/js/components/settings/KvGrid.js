// Сетка «ключевых чисел»: массив {label, value, meta, variant?} → карточки-статы.
// Читабельная альтернатива сырому JSON: важные метрики выносятся сюда, дамп — в <JsonView>.
export default {
  name: 'KvGrid',
  props: {
    items: { type: Array, default: () => [] }, // [{label, value, meta, variant}]
  },
  template: `
    <div class="grid grid--stats" v-if="items.length">
      <div class="stat" v-for="(it, i) in items" :key="i" :class="it.variant === 'accent' && 'stat--accent'">
        <div class="stat__label">{{ it.label }}</div>
        <div class="stat__value" style="font-size:var(--fs-lg)">
          <UiBadge v-if="it.badge" :variant="it.badge" :text="it.value" :dot="false"/>
          <template v-else>{{ it.value }}</template>
        </div>
        <div v-if="it.meta" class="stat__meta">{{ it.meta }}</div>
      </div>
    </div>
    <div v-else class="muted text-sm">Нет данных.</div>
  `,
};
