<template>
  <div v-if="show" :class="'menu '+clazz">
    <div class="x-btn" v-on:click="$emit('close')">X</div>
    <span class="inner">
      <slot />
    </span>
  </div>
</template>

<script>
export default {
  props: ['show', 'clazz']
}
</script>

<style>
</style>
