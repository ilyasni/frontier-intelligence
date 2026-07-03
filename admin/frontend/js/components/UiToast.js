import { toastState, dismissToast } from '../ui.js';

export default {
  name: 'UiToast',
  computed: { items() { return toastState.items; } },
  methods: { dismiss: dismissToast },
  template: `
    <teleport to="body">
      <div class="toasts">
        <div v-for="t in items" :key="t.id" class="toast" :class="'toast--'+t.variant" role="status">
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
