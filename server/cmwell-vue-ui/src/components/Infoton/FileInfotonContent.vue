<template>
  <div>
    <div v-if="content">
      <textarea v-model="newContent" class="file-infoton-content" :readonly="!allowWrite()"></textarea>
      <button v-if="allowWrite()" class="save-button" v-on:click="upload(newContent)">Save</button>
    </div>
    <img v-else-if="isImage" :src="$route.fullPath" />
    <a v-else :href="$route.fullPath" target="_blank">Download Binary FileInfoton</a>
  </div>
</template>

<script>
  export default {
    data() {
      return {
        newContent: ''
      }
    },
    props: ['content', 'mime'],
    computed: {
      isImage() {
        return /image/.test(this.mime)
      }
    },
    methods: {
      allowWrite() {
        return !AppUtils.useAuth || AppUtils.loggedInUser
      },
      upload(content) {
        let headers = { 'Content-Type': this.mime, 'X-CM-Well-Type': 'File' }
        if (AppUtils.token) headers['X-CM-Well-Token'] = AppUtils.token
        fetch(this.$route.fullPath, {
          method: 'post',
          body: content,
          headers
        }).then() // todo feedback to user: was editing successful?
      }
    },
    mounted() {
      this.newContent = this.content
    },
    watch: {
      content(c) {
        this.newContent = c
      }
    }
  }
</script>

<style>
</style>
