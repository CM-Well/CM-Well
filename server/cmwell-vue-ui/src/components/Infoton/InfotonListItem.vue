<template>
  <li :class="'infoton-list-item '+clazz">
    <router-link
      :to="path"
      v-on:click.native="$root.$emit('infotonItemClicked', path)"
    >{{ path | lastPartOf }}</router-link>
  </li>
</template>

<script>
  export default {
    data() {
      return {
        clazz: ''
      }
    },
    props: ['path'],
    filters: {
      lastPartOf: p => p.substring(p.lastIndexOf('/') + 1)
    },
    mounted() {
      this.$root.$on('infotonItemClicked', path => {
        // using a "global" event here because we need collaboration between sibling components...
        this.clazz = path == this.path ? 'selected' : ''
      })
    }
  }
</script>

<style>
</style>
