<template>
  <div class="group-container">
    <div class="group-meta">
      <span v-on:click="remove" class="plus-btn" title="remove">-</span>
      <select v-model="quantifier" v-on:change="changed">
        <option value="must" selected>MUST</option>
        <option value="should">SHOULD</option>
      </select>
      <label>
        <input v-model="not" type="checkbox" v-on:change="changed" />not
      </label>
    </div>
    <div class="group">
      <field-selector
        v-for="s in selectors"
        :key="s"
        :selId="s"
        v-on:changed="changed"
        v-on:remove="removeSelector"
        :fields="fields"
      />
      <div v-on:click="add" class="plus-btn">+</div>
    </div>
  </div>
</template>

<script>
  export default {
    data() {
      return {
        quantifier: 'must',
        not: false,
        selectors: [1],
        nextSelId: 2,
        query: []
      }
    },
    components: { 'field-selector': FieldSelector },
    props: ['groupId', 'fields'],
    methods: {
      add() {
        this.selectors = [...this.selectors, this.nextSelId++]
      },
      changed(id, isValid, fVal) {
        this.query[id] = fVal
        this.$emit(
          'changed',
          this.groupId,
          isValid,
          this.quantifier,
          this.not,
          this.query.filter(Boolean).join(',')
        )
      },
      removeSelector(id) {
        this.selectors = this.selectors.filter(s => s != id)
        this.query[id] = null
      },
      remove() {
        this.$emit('remove', this.groupId)
      }
    }
  }
</script>

<style>
</style>
