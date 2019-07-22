<template>
  <div class="autocomplate-container">
    <input
      type="text"
      v-model="field"
      :placeholder="placeholder"
      v-on:keyup="filterFields"
      v-on:change="$emit('input',field)"
    />
    <br />
    <ul class="autocomplete">
      <li v-for="f in activeFields" v-on:click="field=f;activeFields=[];$emit('input',f)">{{ f }}</li>
    </ul>
  </div>
</template>

<script>
  export default {
    data() {
      return {
        field: '',
        activeFields: []
      }
    },
    // Using HTML5's <datalist> is pretty straightforward, until the moment you're facing >5000 elements in DOM...
    props: ['fields', 'placeholder'],
    methods: {
      filterFields() {
        this.activeFields = this.field
          ? this.fields.filter(
              f => f.toLowerCase().indexOf(this.field.toLowerCase()) != -1
            )
          : []
      }
    }
  }
</script>

<style>
</style>
