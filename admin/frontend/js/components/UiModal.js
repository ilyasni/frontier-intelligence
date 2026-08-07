// Доступная модалка: role=dialog, закрытие по Esc и клику на фон, автофокус, блок скролла фона.
// Слоты: default (тело), footer (кнопки).
//
// 07.08.2026 добавлены три вещи, которых не хватало для клавиатуры и скринридера:
//
//   1. Ловушка фокуса. `aria-modal="true"` сообщает вспомогательной технологии, что
//      остальная страница неактивна, но браузер по нему Tab НЕ удерживает — реальный
//      пользователь клавиатуры уходил табом за диалог, в форму под ним, и продолжал
//      «редактировать» невидимое.
//   2. Возврат фокуса. После закрытия фокус проваливался на <body>: следующий Tab
//      начинал обход страницы с начала, а место, откуда открыли диалог, терялось
//      (WCAG 2.4.3 Focus Order).
//   3. Программное имя. Заголовок был виден глазами, но не связан с диалогом:
//      скринридер объявлял просто «диалог». Теперь aria-labelledby указывает на <h3>.
//
// Идентификатор заголовка генерируется через Vue `useId()` (есть с 3.5, в вендоре
// лежит 3.5.13): счётчик уникален в пределах приложения и стабилен между рендерами,
// то есть двум открытым диалогам не достанется один id.
const { useId } = Vue;

export default {
  name: 'UiModal',
  props: {
    title: { type: String, default: '' },
    size: { type: String, default: '' }, // '' | 'lg'
  },
  emits: ['close'],
  setup() {
    return { titleId: useId() };
  },
  mounted() {
    this._esc = (e) => { if (e.key === 'Escape') this.$emit('close'); };
    document.addEventListener('keydown', this._esc);

    // Ловушка фокуса. Слушаем на фазе всплытия у самого документа: диалог
    // телепортирован в body, так что вложенности в открывший его компонент нет.
    this._trap = (e) => {
      if (e.key !== 'Tab') return;
      const modal = this.$refs.modal;
      if (!modal) return;
      const items = this.focusable(modal);
      if (!items.length) {
        // Фокусировать нечего — не даём табу вынести пользователя за диалог.
        e.preventDefault();
        modal.focus();
        return;
      }
      const first = items[0];
      const last = items[items.length - 1];
      const active = document.activeElement;
      if (!modal.contains(active)) {
        e.preventDefault();
        (e.shiftKey ? last : first).focus();
        return;
      }
      if (e.shiftKey && active === first) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && active === last) { e.preventDefault(); first.focus(); }
    };
    document.addEventListener('keydown', this._trap);

    document.body.style.overflow = 'hidden';
    // Запоминаем, откуда пришли, ДО перевода фокуса внутрь.
    this._returnTo = document.activeElement instanceof HTMLElement ? document.activeElement : null;

    this.$nextTick(() => {
      const modal = this.$refs.modal;
      if (!modal) return;
      // Приоритет автофокуса: поле ввода → первый фокусируемый → сам диалог.
      // Последний вариант нужен, чтобы у диалога только для чтения фокус всё равно
      // оказался внутри: иначе Tab начинал обход со страницы под ним.
      const field = modal.querySelector('input, select, textarea');
      const target = field || this.focusable(modal)[0] || modal;
      if (target && target.focus) target.focus();
    });
  },
  unmounted() {
    document.removeEventListener('keydown', this._esc);
    document.removeEventListener('keydown', this._trap);
    document.body.style.overflow = '';
    // Возврат фокуса на открывший элемент. Проверяем isConnected: строка таблицы,
    // из которой открыли диалог, могла исчезнуть после удаления записи.
    if (this._returnTo && this._returnTo.isConnected && this._returnTo.focus) {
      this._returnTo.focus();
    }
    this._returnTo = null;
  },
  methods: {
    focusable(root) {
      const sel = [
        'a[href]', 'button:not([disabled])', 'input:not([disabled])',
        'select:not([disabled])', 'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])',
      ].join(',');
      return Array.from(root.querySelectorAll(sel))
        .filter((el) => el.offsetParent !== null || el === document.activeElement);
    },
  },
  template: `
    <teleport to="body">
      <div class="modal-backdrop" @mousedown.self="$emit('close')">
        <div class="modal" :class="size === 'lg' && 'modal--lg'" role="dialog" aria-modal="true"
             :aria-labelledby="title ? titleId : null" :aria-label="title ? null : 'Диалог'"
             tabindex="-1" ref="modal">
          <div class="modal__head">
            <h3 :id="titleId">{{ title }}</h3>
            <button class="btn btn--icon btn--ghost" @click="$emit('close')" aria-label="Закрыть">✕</button>
          </div>
          <div class="modal__body"><slot/></div>
          <div class="modal__foot" v-if="$slots.footer"><slot name="footer"/></div>
        </div>
      </div>
    </teleport>
  `,
};
