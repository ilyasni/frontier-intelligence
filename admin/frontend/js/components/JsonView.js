// Подсвеченный JSON вместо сырого JSON.stringify. Данные экранируются ДО подсветки — v-html безопасен.
function esc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
function highlight(obj) {
  const json = esc(JSON.stringify(obj, null, 2));
  return json.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?)/g,
    (m) => {
      let cls = 'n';
      if (/^"/.test(m)) cls = /:\s*$/.test(m) ? 'k' : 's';
      else if (/^(true|false|null)$/.test(m)) cls = 'b';
      return `<span class="${cls}">${m}</span>`;
    }
  );
}
export default {
  name: 'JsonView',
  props: { data: { default: null } },
  computed: {
    html() {
      try { return highlight(this.data); } catch { return esc(String(this.data)); }
    },
  },
  template: `<pre class="json-view" v-html="html"></pre>`,
};
