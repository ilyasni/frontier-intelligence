export default {
  name: 'UiBadge',
  props: {
    variant: { type: String, default: 'neutral' },
    text: { type: [String, Number], default: '' },
    dot: { type: Boolean, default: true },
  },
  template: `<span class="badge" :class="'badge--'+variant"><span v-if="dot" class="dot"></span>{{ text }}<slot/></span>`,
};
