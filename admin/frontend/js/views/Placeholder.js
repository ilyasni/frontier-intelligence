export default {
  name: 'Placeholder',
  computed: { title() { return (this.$route.meta && this.$route.meta.title) || 'Экран'; } },
  template: `
    <div class="card"><div class="card__body">
      <div class="state">
        <div class="state__icon">🚧</div>
        <div class="state__title">{{ title }}</div>
        <div class="state__msg">Раздел переносится на новый интерфейс — скоро будет здесь.</div>
      </div>
    </div></div>
  `,
};
