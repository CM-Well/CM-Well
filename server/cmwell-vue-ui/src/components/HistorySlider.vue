<template>
  <div v-if="versions.length">
    <input
      v-if="versions.length"
      class="history-slider"
      type="range"
      min="0"
      step="1"
      :max="versions.length - 1"
      v-model="value"
    />
    <div class="history-status">{{ versions.length }} versions</div>
  </div>
  <div v-else class="history-status">{{ status }}</div>
</template>

<script>
  export default {
    data() {
      return {
        versions: [],
        value: 0,
        status: 'loading...'
      }
    },
    watch: {
      value(n) {
        this.$emit('historyVersion', this.versions[n])
      }
    },
    mounted() {
      this.status = 'loading...'
      mfetch(this.$route.fullPath + '?with-history&format=jsonl')
        .then(resp => {
          if (resp.status != 200) {
            this.status = `HTTP ${resp.status} while loading history`
            return null
          }
          return resp.clone().json()
        })
        .then(history => {
          if (!history) return
          if (history.versions.length == 1) {
            this.status = 'This is the only version of this Infoton.'
            return
          }
          if (history.versions.length > 100) {
            this.status = `Error: This Infoton has ${history.versions.length} historical versions. That's too many.`
            return
          }
          this.versions = history.versions.sortBy(
            i => i['lastModified.sys'][0].value
          )
          this.value = this.versions.length - 1
        })
    }
  }
</script>

<style>
</style>
