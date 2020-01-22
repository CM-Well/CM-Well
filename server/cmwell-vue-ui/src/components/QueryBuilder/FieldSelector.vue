<template>
  <div class="field-selector">
    <span v-on:click="remove" class="plus-btn" title="remove">-</span>
    <select v-model="quantifier" v-on:change="updateParent">
      <option value="must" selected>MUST</option>
      <option value="should">SHOULD</option>
    </select>
    <label>
      <input v-model="not" type="checkbox" v-on:change="updateParent" />not
    </label>
    <autocomplete
      v-model="field"
      :fields="fields"
      placeholder="field name"
      v-on:input="updateParent"
    />
    <select v-model="op" v-on:change="updateParent">
      <option value="::" selected>Exact Match</option>
      <option value=":">Containts</option>
      <option value=">>">&gt;</option>
      <option value=">">&gt;=</option>
      <option value="<<">&lt;</option>
      <option value="<">&lt;=</option>
    </select>
    <input
      v-model="fvalue"
      type="text"
      class="fvalue"
      v-on:change="updateParent"
      placeholder="value"
    />
  </div>
</template>

<script>
  export default {
    data() {
      return {
        quantifier: 'must',
        not: false,
        field: '',
        op: '::',
        fvalue: ''
      }
    },
    components: { autocomplete: AutoComplete },
    props: ['selId', 'fields'],
    methods: {
      updateParent() {
        let wrap = this.quantifier == 'should' && this.not
        let toString = `${this.quantifier === 'should' ? '*' : ''}${
          wrap ? '[' : ''
        }${this.not ? '-' : ''}${this.field}${this.op}${this.fvalue}${
          wrap ? ']' : ''
        }`
        let isValid = this.field && (this.fvalue || this.op === ':')
        this.$emit('changed', this.selId, isValid, toString)
      },
      remove() {
        this.$emit('remove', this.selId)
      }
    }
  }
</script>

<style>
</style>
