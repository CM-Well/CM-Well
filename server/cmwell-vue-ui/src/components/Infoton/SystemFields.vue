<template>
  <table class="system">
    <tr v-for="(value,key) in fields" :key="key">
      <td>{{ key }}</td>
      <td>{{ value }}</td>
    </tr>
  </table>
</template>

<script>
export default {
  props: ['fields']
}
</script>

<style>
</style>
