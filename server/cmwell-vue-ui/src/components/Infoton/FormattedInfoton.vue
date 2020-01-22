<template>
  <div class="formatted-infoton">
    <div class="close-button" v-on:click="$emit('close')" title="close">X</div>
    <textarea v-model="payload" :class="'content '+clazz" :readonly="!editMode"></textarea>
    <transition name="fade">
      <span v-if="!editMode && allowWrite()" v-on:click="switchToEditMode()">
        <router-link to>Edit</router-link>
      </span>
    </transition>
    <transition name="fade">
      <button v-if="delayedEditMode" class="save-button" v-on:click="ingest()">Ingest</button>
    </transition>
    <span class="ingest-status">{{ status }}</span>
  </div>
</template>

<script>
  export default {
    data() {
      return {
        payload: '',
        editMode: false,
        delayedEditMode: false,
        status: ''
      }
    },
    props: ['format'],
    watch: {
      format() {
        this.load()
      }
    },
    computed: {
      clazz() {
        return this.editMode ? 'edit-mode' : ''
      }
    },
    mounted() {
      this.load()
    },
    methods: {
      allowWrite() {
        return !AppUtils.useAuth || AppUtils.loggedInUser
      },
      load() {
        this.editMode = this.delayedEditMode = false
        this.status = 'Loading...'
        this.payload = ''
        mfetch(`${this.$route.fullPath}?format=${this.format}`)
          .then(resp => resp.clone().text())
          .then(data => {
            this.payload = data
            this.status = ''
          })
      },
      switchToEditMode() {
        this.editMode = true
        setTimeout(() => {
          this.delayedEditMode = true
        }, 500)
      },
      ingest() {
        this.status = 'Data ingest in progress...'
        let headers = AppUtils.token ? { 'X-CM-Well-Token': AppUtils.token } : {}
        fetch(`/_in?format=${this.format}`, {
          method: 'post',
          body: this.payload,
          headers
        }).then(resp => {
          if (resp.status === 200) this.status = 'Successfully ingested.'
          else
            resp
              .text()
              .then(
                body =>
                  (this.status = `Error in ingest: HTTP ${resp.status}, ${body}`)
              )
        })
      }
    }
  }
</script>

<style>
</style>
