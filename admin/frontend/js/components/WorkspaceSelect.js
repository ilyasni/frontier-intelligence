import { store, loadWorkspaces } from '../store.js';

export default {
  name: 'WorkspaceSelect',
  props: {
    modelValue: { default: '' },
    allowAll: { type: Boolean, default: true },
    allLabel: { type: String, default: 'Все workspace' },
  },
  emits: ['update:modelValue'],
  computed: {
    workspaces() { return store.workspaces; },
  },
  mounted() { loadWorkspaces().catch(() => {}); },
  template: `
    <select class="select" :value="modelValue" @change="$emit('update:modelValue', $event.target.value)">
      <option v-if="allowAll" value="">{{ allLabel }}</option>
      <option v-for="w in workspaces" :key="w.id" :value="w.id">{{ w.name || w.id }}</option>
    </select>
  `,
};
