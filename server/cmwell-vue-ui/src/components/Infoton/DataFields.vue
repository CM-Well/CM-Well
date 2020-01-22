<template>
  <table class="data">
    <tr v-for="field in filterFields(fields)" :key="field[0]">
      <td>{{ field[0] | noNn }}</td>
      <td>
        <field-value v-for="value in filterValues(field[1])" :value="value" :key="value.hash" />
      </td>
    </tr>
  </table>
</template>

<script>
import FieldValue from '@/components/Infoton/FieldValue.vue'
export default {
  // TODO do we want to persist quadFilter between Infotons? e.g. Settings.quadFilter ? Makes sense with the "default" choice, what about
  //      speifiec quad(s)? Should we allow "1 or more quads to filter by" or is it an overkill?
  components: { 'field-value': FieldValue },
  props: ['fields', 'quadFilter'],
  filters: {
    noNn: v => v.replace(/.nn$/, '')
  },
  methods: {
    filterFields(fs) {
      return fs.filter(f => this.filterValues(f[1]).length)
    },
    filterValues(vs) {
      return vs.filter(
        v =>
          this.quadFilter === 'all' ||
          (this.quadFilter === 'default' && !v.quad) ||
          this.quadFilter === v.quad
      )
    }
  }
}
</script>

<style>
</style>
