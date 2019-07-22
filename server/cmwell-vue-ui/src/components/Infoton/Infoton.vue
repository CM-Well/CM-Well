<template>
  <div v-if="this.err" class="infoton error">
    <img class="error-icon" src="assets/errorIcon.svg" />
    HTTP {{ err.status }}: {{ err.msg }}
  </div>
  <div v-else-if="$route.fullPath!='/'" class="infoton">
    <span
      v-if="$route.fullPath!='/proc/node'"
      class="hamburger"
      v-on:click="showMenu=!showMenu"
    >&#9776;</span>
    <msg :show="showMenu" v-on:close="showMenu=false" clazz>
      <span v-on:click="showMenu=false; showHistory=true">
        <router-link to>View history</router-link>
      </span>
      <hr />
      <span v-if="allowWrite()" v-on:click="showMenu=false; deleteInfoton()">
        <router-link to class="danger">Delete this Infoton</router-link>
        <br />
        <span class="hint">Note: it will still be availabe in app until refresh.</span>
      </span>
      <span v-else class="disabled" title="You don't have a Write Permission">Delete this Infoton</span>
      <hr />
      <span v-if="quadsToFilterBy.length">
        Filter fields by quads (subgraphs):
        <br />
        <select v-on:change="showMenu=false" v-model="quadFilter">
          <option value="all" selected key="all">No filtering</option>
          <option value="default" key="default">Only show default graph</option>
          <option v-for="q in quadsToFilterBy" :value="q" :key="q">{{ q | lastPartOf }}</option>
        </select>
        <hr />
      </span>
      View/Edit Infoton using RDF format:
      <ul>
        <li v-for="format in formats" v-on:click="showMenu=false; currentFormat=format.value">
          <router-link to>{{ format.displayName }}</router-link>
        </li>
      </ul>
      <span v-if="gm()" class="gm">
        <a :href="'/_cas/'+system.uuid+'?override-mimetype'" target="_blank">_cas</a>&nbsp;
        <a :href="'/_es/'+system.uuid+'?override-mimetype'" target="_blank">_es</a>&nbsp;
        <br />
        <span v-for="op in ['x-info','x-verify','x-fix','purge-all']" :key="op">
          <a :href="'?op='+op" target="_blank">{{op}}</a>&nbsp;
        </span>
      </span>
    </msg>

    <h2>
      <img
        v-if="isFileInfoton"
        class="file-infoton-icon"
        title="This is a FileInfoton"
        src="assets/fileInfotonIcon.svg"
      />
      {{ title }}
    </h2>
    <history-slider v-if="showHistory && !currentFormat" v-on:historyVersion="historyVersion" />

    <span
      v-if="!currentFormat"
      class="sys-button"
      v-on:click="toggleState()"
      title="Toggle System Fields"
    >&#9881;</span>
    <system-fields v-if="!currentFormat" :fields="shownSystem" />
    <img
      v-if="!currentFormat && quadFilter!='all' && fields.length"
      class="filter-icon"
      title="Filtering fields"
      src="assets/filterIcon.svg"
    />
    <data-fields v-if="!currentFormat" :fields="fields" :quadFilter="quadFilter" />

    <file-infoton-content
      v-if="isFileInfoton && !currentFormat"
      :content="content"
      :mime="mimeType"
    />

    <formatted-infoton v-if="currentFormat" :format="currentFormat" v-on:close="currentFormat=''" />
  </div>
</template>

<script>
  import SystemFields from '@/components/Infoton/SystemFields.vue'
  import DataFields from '@/components/Infoton/DataFields.vue'
  import HistorySlider from '@/components/HistorySlider.vue'
  import FileInfotonContent from '@/components/Infoton/FileInfotonContent.vue'
  import FormattedInfoton from '@/components/Infoton/FormattedInfoton.vue'
  import Msg from '@/components/Message.vue'
  export default {
    data() {
      return {
        system: {},
        fields: [],
        title: '',
        sysFieldsState: 0,
        shownSystem: {},
        err: null,
        content: '',
        isFileInfoton: false,
        mimeType: '',
        showHistory: false,
        showMenu: false,
        formats: AppUtils.formats,
        currentFormat: '',
        quadFilter: 'all'
      }
    },
    components: {
      'system-fields': SystemFields,
      'data-fields': DataFields,
      'history-slider': HistorySlider,
      'file-infoton-content': FileInfotonContent,
      'formatted-infoton': FormattedInfoton,
      Msg
    },
    watch: {
      '$route.path': function(path) {
        this.reset(path)
      }
    },
    mounted() {
      this.reset(this.$route.path)
    },
    filters: {
      lastPartOf: p => (p ? p.substring(p.lastIndexOf('/') + 1) : '')
    },
    computed: {
      quadsToFilterBy() {
        let quads = this.fields.flatMap(i => i[1].map(v => v.quad))
        return [...new Set(quads)]
      }
    },
    methods: {
      reset(path) {
        this.showHistory = false
        mfetch(`${path}?format=jsonl`)
          .then(r => {
            if (r.status != 200)
              r.text().then(msg => (this.err = { status: r.status, msg }))
            else return r.clone().json()
          })
          .then(infotonL => {
            if (!infotonL || path != this.$route.path) return
            this.err = null
            let infoton = this.fromJSONL(infotonL)
            this.updateWith(infoton)
          })
      },
      allowWrite() {
        return !AppUtils.useAuth || AppUtils.loggedInUser
      },
      lastPartOf: p => p.substring(p.lastIndexOf('/') + 1),
      updateWith(infoton) {
        this.system = infoton.system
        this.fields = infoton.fields || {}
        this.isFileInfoton = infoton.system.type === 'FileInfoton'
        this.content = infoton.content
        this.mimeType = infoton.mimeType
        this.title = this.lastPartOf(this.$route.path)
        this.sysFieldsState = +Settings['sysfields-state']
        this.updateShownSysFields()
        this.currentFormat = ''
      },
      fromJSONL(infotonL) {
        let keys = Object.keys(infotonL),
          sysKeys = keys.filter(
            k =>
              k.endsWith('.sys') &&
              k.indexOf('.content.sys') === -1 &&
              k != '@id.sys'
          ),
          dataKeys = keys.filter(k => !k.endsWith('.sys'))
        let system = {},
          fields = []
        sysKeys.forEach(
          k => (system[k.replace(/.sys/, '')] = infotonL[k][0].value)
        )
        dataKeys.sort().forEach(k => {
          var field = [k, infotonL[k]]
          for (let i = 0; i < field[1].length; i++)
            field[1][i].hash = AppUtils.hex(
              field[1][i].value + field[1][i].type + (field[1][i].quad || '')
            )
          fields.push(field)
        })
        let content = infotonL['data.content.sys']
          ? infotonL['data.content.sys'][0].value
          : ''
        let mimeType = infotonL['mimeType.content.sys']
          ? infotonL['mimeType.content.sys'][0].value
          : ''
        return { system, fields, content, mimeType }
      },
      gm: () => !!AppUtils.gm,
      toggleState() {
        this.sysFieldsState = (this.sysFieldsState + 1) % 3
        Settings['sysfields-state'] = this.sysFieldsState
        this.updateShownSysFields()
      },
      updateShownSysFields() {
        switch (this.sysFieldsState) {
          case 1:
            this.shownSystem = {
              uuid: this.system.uuid,
              lastModified: this.system.lastModified
            }
            break
          case 2:
            this.shownSystem = {}
            break
          case 0:
          default:
            this.shownSystem = this.system
        }
      },
      historyVersion(version) {
        this.updateWith(this.fromJSONL(version))
      },
      deleteInfoton() {
        let headers = AppUtils.token ? { 'X-CM-Well-Token': AppUtils.token } : {}
        fetch(this.system.path, { method: 'delete', headers }).then(() => {
          this.$router.push(this.system.parent)
        })
      }
    }
  }
</script>

<style>
</style>
