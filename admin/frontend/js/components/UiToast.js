import { toastState, dismissToast } from '../ui.js';

export default {
  name: 'UiToast',
  computed: { items() { return toastState.items; } },
  methods: { dismiss: dismissToast },
  template: `
    <teleport to="body">
      <!-- aria-live висит на ПОСТОЯННОМ контейнере, а не на самом сообщении.
           Раньше role="status" стоял внутри v-for, то есть живой регион создавался
           вместе со своим содержимым — а такое добавление скринридеры не объявляют:
           регион обязан существовать в DOM заранее и меняться потом. Контейнер
           смонтирован в AppLayout вне ветки авторизации, то есть живёт всю сессию. -->
      <div class="toasts" role="status" aria-live="polite" aria-atomic="false">
        <div v-for="t in items" :key="t.id" class="toast" :class="'toast--'+t.variant">
          <div class="toast__body">
            <div class="toast__title">{{ t.title }}</div>
            <div v-if="t.msg" class="toast__msg">{{ t.msg }}</div>
          </div>
          <button class="toast__close" @click="dismiss(t.id)" aria-label="Закрыть">✕</button>
        </div>
      </div>
    </teleport>
  `,
};
