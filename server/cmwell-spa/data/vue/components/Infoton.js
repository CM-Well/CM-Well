const SystemFields = {
    template: `<table class="system">
                    <tr v-for="(value,key) in fields" :key="key">
                        <td>{{ key }}</td><td>{{ value }}</td>
                    </tr>
                </table>`,
    props: ['fields']
}


//todo quad icon
const FieldValue = {
    props: ['value', 'quadFilter', 'fieldName'],
    data() { return { isURL: this.value.type === 'http://www.w3.org/2001/XMLSchema#anyURI', isInternal: false } },
    template: `<div>
                    <a v-if="this.isURL && !this.isInternal" :href="value.value" target="_blank">{{ value.value }} <img class="link-out" src='data:image/svg+xml;utf8,<svg height="1024" width="768" xmlns="http://www.w3.org/2000/svg"><path d="M640 768H128V257.90599999999995L256 256V128H0v768h768V576H640V768zM384 128l128 128L320 448l128 128 192-192 128 128V128H384z"/></svg>'/></a>
                    <router-link v-if="this.isURL && this.isInternal" v-on:click.native="addToTraversalGraph(fieldName, value.value)" :to="value.value | asPath">{{ value.value }}</router-link>
                    <span v-if="!this.isURL">{{ value.value }}</span>
                    <span class="quad" v-if="value.quad" :title="value.quad">
                        <a :href="'/?op=search&recursive&qp=system.quad::' + value.quad" target="_blank">{{ value.quad | lastPartOf }}</a>
                    </span>
                </div>`,
    beforeMount() {
        if(this.isURL) AppUtils.topLevelDomains.then(tlds => {
            let path = AppUtils.pathFromURL(this.value.value), domain = path.substring(1,path.indexOf('/',1))
            this.isInternal = tlds.includes(domain)
        })
    },
    filters: {
        asPath: AppUtils.pathFromURL,
        lastPartOf: p => p.substring(p.lastIndexOf('/')+1)
    },
    methods: {
        addToTraversalGraph: function(fieldName, path) {
            this.$root.$emit('addToTraversal', path, path.substring(path.lastIndexOf('/')+1), fieldName)
        }
    }
}

// TODO do we want to persist quadFilter between Infotons? e.g. Settings.quadFilter ? Makes sense with the "default" choice, what about 
//      speifiec quad(s)? Should we allow "1 or more quads to filter by" or is it an overkill?
const DataFields = {
    components: { 'field-value': FieldValue },
    template: `<table class="data">
                    <tr v-for="field in filterFields(fields)" :key="field[0]">
                        <td>{{ field[0] | noNn }}</td>
                        <td><field-value v-for="value in filterValues(field[1])" :value="value" :key="value.hash" :fieldName="field[0]"/></td>
                    </tr>
                </table>`,
    props: ['fields', 'quadFilter'],
    filters: {
        noNn: v => v.replace(/.nn$/,'')
    },
    methods: {
        filterFields(fs) {
            return fs.filter(f => this.filterValues(f[1]).length)
        },
        filterValues(vs) {
            return vs.filter(v => this.quadFilter === 'all' || (this.quadFilter === 'default' && !v.quad) || this.quadFilter === v.quad)
        }
    }
}

const HistorySlider = {
    data: () => ({ versions: [], value: 0, status: 'loading...' }),
    template: `<div v-if="versions.length">
                    <input v-if="versions.length" class="history-slider" type="range" min="0" step="1" :max="versions.length-1" v-model="value"/>
                    <div class="history-status">{{ versions.length }} versions</div>
                </div>
                <div v-else class="history-status">{{ status }}</div>`,
    watch: {
        value(n) { this.$emit('historyVersion', this.versions[n]) },
    },
    mounted() { 
        this.status = 'loading...'
        mfetch(this.$route.fullPath + '?with-history&format=jsonl').then(resp => {
            if(resp.status!=200) {
                this.status = `HTTP ${resp.status} while loading history`
                return null
            }
            return resp.clone().json()
        }).then(history => {
            if(!history)
                return
            if(history.versions.length == 1) {
                this.status = 'This is the only version of this Infoton.'
                return
            }
            if(history.versions.length > 100) {
                this.status = `Error: This Infoton has ${history.versions.length} historical versions. That's too many.`
                return
            } 
            this.versions = history.versions.sortBy(i => i['lastModified.sys'][0].value)
            this.value = this.versions.length-1
        })        
    }
}

const FileInfotonContent = {
    props: ['content', 'mime'],
    data: () => ({
        newContent: ''
    }),
    computed: {
        isImage() { return /image/.test(this.mime) }
    },
    methods: {
        allowWrite() { return !AppUtils.useAuth || AppUtils.loggedInUser },
        upload(content) {
            let headers = { 'Content-Type': this.mime, 'X-CM-Well-Type': 'File' }
            if(AppUtils.token) headers['X-CM-Well-Token'] = AppUtils.token
            fetch(this.$route.fullPath, {
                method: 'post',
                body: content,
                headers
            }).then() // todo feedback to user: was editing successful?
        }
    },
    mounted() { this.newContent = this.content },
    watch: { content(c) { this.newContent = c } },
    template: ` <div v-if="content">
                    <textarea v-model="newContent" class="file-infoton-content" :readonly="!allowWrite()"></textarea>
                    <button v-if="allowWrite()" class="save-button" v-on:click="upload(newContent)"> Save </button>
                </div>
                <img v-else-if="isImage" :src="$route.fullPath" />
                <a v-else :href="$route.fullPath" target="_blank">Download Binary FileInfoton</a>`
}

const FormattedInfoton = {
    props: ['format'],
    data() { return {
        payload: '',
        editMode: false,
        delayedEditMode: false,
        status: ''
    } },
    template: `<div class="formatted-infoton">
                    <div class="close-button" v-on:click="$emit('close')" title="close">X</div>
                    <textarea v-model="payload" :class="'content '+clazz" :readonly="!editMode"></textarea>
                    <transition name="fade">
                        <span v-if="!editMode && allowWrite()" v-on:click="switchToEditMode()"><router-link to>Edit</router-link></span>
                    </transition>
                    <transition name="fade">
                        <button v-if="delayedEditMode" class="save-button" v-on:click="ingest()"> Ingest </button>
                    </transition>
                    <span class="ingest-status">{{ status }}</span>
                </div>`,
    watch: { format() { this.load() } },
    computed: {
      clazz() { return this.editMode ? 'edit-mode' : '' }  
    },
    mounted() { this.load() },
    methods: {
        allowWrite() { return !AppUtils.useAuth || AppUtils.loggedInUser },
        load() {
            this.editMode = this.delayedEditMode = false
            this.status = 'Loading...'
            this.payload = ''
            mfetch(`${this.$route.fullPath}?format=${this.format}`).then(resp => resp.clone().text()).then(data => {
                this.payload = data
                this.status = ''
            })            
        },
        switchToEditMode() {
            this.editMode = true
            setTimeout(() => { this.delayedEditMode = true }, 500)
        },
        ingest() {
            this.status = 'Data ingest in progress...'
            let headers = AppUtils.token ? { 'X-CM-Well-Token': AppUtils.token } : { }
            fetch(`/_in?format=${this.format}`, { method: 'post', body: this.payload, headers }).then(resp => {
                if(resp.status === 200)
                    this.status = 'Successfully ingested.'
                else
                    resp.text().then(body => this.status = `Error in ingest: HTTP ${resp.status}, ${body}`)
            })
        }
    }
}

Vue.component('infoton', {
    components: { 'system-fields': SystemFields, 'data-fields': DataFields, 'history-slider': HistorySlider, 'file-infoton-content': FileInfotonContent, 'formatted-infoton': FormattedInfoton },
    data: () => ({ system: {}, fields: [], title: "", sysFieldsState: 0, shownSystem: {}, err: null, content: '', isFileInfoton: false, mimeType: '',
                   showHistory: false, showMenu: false, formats: AppUtils.formats, currentFormat: '', quadFilter: 'all'
                 }),
    template: ` <div v-if="this.err" class="infoton error">
                    <img class="error-icon" src='data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" version="1.0" width="200.000000pt" height="200.000000pt" viewBox="0 0 200.000000 200.000000" preserveAspectRatio="xMidYMid meet"><g transform="translate(0.000000,200.000000) scale(0.100000,-0.100000)" fill="%23000000" stroke="none"><path d="M182 1883 c-18 -9 -42 -29 -54 -46 -23 -33 -35 -116 -23 -164 4 -18 79 -101 208 -231 l202 -202 20 42 c12 24 29 54 38 68 l16 26 -174 174 c-96 96 -175 181 -175 188 0 7 7 15 16 19 11 4 68 -47 199 -177 l183 -183 -28 -36 c-70 -87 -90 -204 -54 -313 15 -47 39 -76 217 -256 219 -220 251 -243 352 -250 l60 -5 -66 71 c-40 42 -72 86 -79 109 -9 28 -53 80 -170 198 -87 88 -163 170 -169 182 -24 47 -12 122 27 166 15 16 17 16 39 -5 13 -12 23 -27 23 -35 0 -16 -22 -17 -38 -1 -35 35 -53 -84 -19 -135 35 -54 131 -26 184 52 25 37 31 109 12 158 -12 32 -540 567 -581 589 -36 19 -125 17 -166 -3z"/><path d="M1185 1808 c-3 -8 -4 -63 -3 -124 l3 -109 30 0 30 0 0 120 0 120 -28 3 c-16 2 -29 -2 -32 -10z"/><path d="M897 1734 c-16 -16 -5 -50 41 -130 47 -84 65 -100 96 -88 27 10 18 45 -33 134 -37 66 -54 86 -73 88 -13 2 -27 0 -31 -4z"/><path d="M1473 1728 c-32 -41 -93 -160 -93 -183 0 -29 24 -43 49 -28 25 16 112 174 109 197 -4 25 -49 34 -65 14z"/><path d="M1616 1482 c-84 -50 -103 -69 -93 -96 11 -27 52 -18 139 32 74 43 83 51 83 78 0 46 -36 42 -129 -14z"/><path d="M840 1444 c0 -4 25 -33 55 -66 30 -32 58 -73 63 -91 7 -28 19 -37 96 -75 86 -41 89 -42 108 -25 27 24 48 67 48 97 0 21 -14 31 -106 75 -135 64 -264 106 -264 85z"/><path d="M1580 1224 c-10 -11 -11 -20 -3 -32 9 -14 29 -17 129 -17 l119 0 0 30 0 30 -116 3 c-97 2 -118 0 -129 -14z"/><path d="M1229 1177 c-26 -14 -45 -32 -47 -44 -2 -11 15 -53 37 -94 44 -83 81 -185 81 -225 0 -15 -10 -42 -21 -61 l-21 -34 -24 22 c-13 12 -24 28 -24 36 0 15 26 18 35 3 25 -41 34 56 11 120 -10 29 -18 36 -44 38 -44 4 -105 -32 -131 -78 -25 -45 -28 -127 -7 -168 25 -47 568 -580 599 -588 48 -11 131 1 164 24 17 12 38 37 47 56 18 39 21 127 5 159 -12 24 -399 417 -411 417 -3 0 -9 -13 -13 -30 -4 -16 -18 -46 -32 -66 l-25 -37 178 -178 c126 -126 175 -182 171 -193 -4 -9 -12 -16 -19 -16 -7 0 -96 83 -198 185 l-184 184 26 28 c97 101 89 242 -27 473 -51 101 -56 104 -126 67z"/><path d="M1527 1033 c-4 -3 -7 -16 -7 -29 0 -18 18 -33 90 -74 100 -57 133 -62 138 -18 3 23 -6 31 -83 77 -81 47 -122 61 -138 44z"/></g></svg>'/>
                    HTTP {{ err.status }}: {{ err.msg }}
                </div>
                <div v-else-if="$route.fullPath!='/'" class="infoton">
                    <span v-if="$route.fullPath!='/proc/node'" class="hamburger" v-on:click="showMenu=!showMenu">&#9776;</span>
                    <msg :show="showMenu" v-on:close="showMenu=false" clazz="">
                        <span v-on:click="showMenu=false; showHistory=true"><router-link to>View history</router-link></span>
                        <hr/>
                        <span v-if="allowWrite()" v-on:click="showMenu=false; deleteInfoton()">
                            <router-link to class="danger">Delete this Infoton</router-link><br/>
                            <span class="hint">Note: it will still be availabe in app until refresh.</span>
                        </span>
                        <span v-else class="disabled" title="You don't have a Write Permission">
                            Delete this Infoton
                        </span>
                        <hr/>
                        <span v-if="quadsToFilterBy.length">
                            Filter fields by quads (subgraphs):<br/>
                            <select v-on:change="showMenu=false" v-model="quadFilter">
                                <option value="all" selected key="all">No filtering</option>
                                <option value="default" key="default">Only show default graph</option>
                                <option v-for="q in quadsToFilterBy" :value="q" :key="q">{{ q | lastPartOf }}</option>
                            </select>
                            <hr/>
                        </span>
                        View/Edit Infoton using RDF format:
                        <ul><li v-for="format in formats" v-on:click="showMenu=false; currentFormat=format.value"><router-link to>{{ format.displayName }}</router-link></li></ul>
                        <span v-if="gm()" class="gm">
                            <a :href="'/_cas/'+system.uuid+'?override-mimetype'" target="_blank">_cas</a>&nbsp;
                            <a :href="'/_es/'+system.uuid+'?override-mimetype'" target="_blank">_es</a>&nbsp;
                            <br/>
                            <span v-for="op in ['x-info','x-verify','x-fix','purge-all']" :key="op"><a :href="'?op='+op" target="_blank">{{op}}</a>&nbsp;</span>
                        </span>
                    </msg>

                    <h2>
                        <img v-if="isFileInfoton" class="file-infoton-icon" title="This is a FileInfoton" src='data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="0 0 40 40" version="1.1" width="40px" height="40px"><g id="surface1"><path style=" fill:%23FFFFFF;" d="M 6.5 37.5 L 6.5 2.5 L 24.792969 2.5 L 33.5 11.207031 L 33.5 37.5 Z "/><path style="fill: %23011EFF;" d="M 24.585938 3 L 33 11.414063 L 33 37 L 7 37 L 7 3 L 24.585938 3 M 25 2 L 6 2 L 6 38 L 34 38 L 34 11 Z "/><path style="fill: rgba(1, 30, 255, 0.25);" d="M 24.5 11.5 L 24.5 2.5 L 24.792969 2.5 L 33.5 11.207031 L 33.5 11.5 Z "/><path style="fill: %23011EFF;" d="M 25 3.414063 L 32.585938 11 L 25 11 L 25 3.414063 M 25 2 L 24 2 L 24 12 L 34 12 L 34 11 Z "/><path style="fill: %23011EFF;" d="M 27.5 17 L 12.5 17 C 12.222656 17 12 16.777344 12 16.5 C 12 16.222656 12.222656 16 12.5 16 L 27.5 16 C 27.777344 16 28 16.222656 28 16.5 C 28 16.777344 27.777344 17 27.5 17 Z "/><path style="fill: %23011EFF;" d="M 23.5 20 L 12.5 20 C 12.222656 20 12 19.777344 12 19.5 C 12 19.222656 12.222656 19 12.5 19 L 23.5 19 C 23.777344 19 24 19.222656 24 19.5 C 24 19.777344 23.777344 20 23.5 20 Z "/><path style="fill: %23011EFF;" d="M 27.5 23 L 12.5 23 C 12.222656 23 12 22.777344 12 22.5 C 12 22.222656 12.222656 22 12.5 22 L 27.5 22 C 27.777344 22 28 22.222656 28 22.5 C 28 22.777344 27.777344 23 27.5 23 Z "/><path style="fill: %23011EFF;" d="M 23.5 26 L 12.5 26 C 12.222656 26 12 25.777344 12 25.5 C 12 25.222656 12.222656 25 12.5 25 L 23.5 25 C 23.777344 25 24 25.222656 24 25.5 C 24 25.777344 23.777344 26 23.5 26 Z "/><path style="fill: %23011EFF;" d="M 27.5 29 L 12.5 29 C 12.222656 29 12 28.777344 12 28.5 C 12 28.222656 12.222656 28 12.5 28 L 27.5 28 C 27.777344 28 28 28.222656 28 28.5 C 28 28.777344 27.777344 29 27.5 29 Z "/></g></svg>'/>
                        {{ title }}
                    </h2>
                    <history-slider v-if="showHistory && !currentFormat" v-on:historyVersion="historyVersion" />

                    <span v-if="!currentFormat" class="sys-button" v-on:click="toggleState()" title="Toggle System Fields">&#9881;</span>
                    <system-fields v-if="!currentFormat" :fields="shownSystem" />
                    <img v-if="!currentFormat && quadFilter!='all' && fields.length" class="filter-icon" title="Filtering fields" src='data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" style="enable-background:new 0 0 50 50;" version="1.1" viewBox="0 0 50 50" xml:space="preserve"><g id="Layer_1"><path d="M1,10.399l19,20v18.405l10-6.25V30.399l19-20V1H1V10.399z M3,3h44v6.601l-19,20v11.845l-6,3.75V29.601l-19-20V3z"/></g><g/></svg>'/>
                    <data-fields v-if="!currentFormat" :fields="fields" :quadFilter="quadFilter" />

                    <file-infoton-content v-if="isFileInfoton && !currentFormat" :content="content" :mime="mimeType" />

                    <formatted-infoton v-if="currentFormat" :format="currentFormat" v-on:close="currentFormat=''" />
                </div>`,
    watch: { '$route.path': function(path) {
        this.showHistory = false
        mfetch(`${path}?format=jsonl`).then(r => {
            if(r.status!=200)
                r.text().then(msg => this.err = { status: r.status, msg })
            else
                return r.clone().json()
        }).then(infotonL => {
            if(!infotonL || path != this.$route.path) return
            this.err = null
            let infoton = this.fromJSONL(infotonL)
            this.updateWith(infoton)
        })
    } },
    filters: {
        lastPartOf: p => p ? p.substring(p.lastIndexOf('/')+1) : ''
    },
    computed: {
      quadsToFilterBy() {
          let quads = this.fields.flatMap(i => i[1].map(v => v.quad))
          return [...new Set(quads)]
      }
    },
    methods: {
        allowWrite() { return !AppUtils.useAuth || AppUtils.loggedInUser }, 
        lastPartOf: p => p.substring(p.lastIndexOf('/')+1),
        updateWith(infoton) {
            this.system = infoton.system
            this.fields = infoton.fields || {}
            this.isFileInfoton = infoton.system.type === 'FileInfoton'
            this.content = infoton.content
            this.mimeType = infoton.mimeType
            this.title = this.lastPartOf(this.$route.fullPath)
            this.sysFieldsState = +Settings['sysfields-state']
            this.updateShownSysFields()
            this.currentFormat = ''
        },
        fromJSONL(infotonL) {
            let keys = Object.keys(infotonL)
                        , sysKeys = keys.filter(k => k.endsWith(".sys") && k.indexOf(".content.sys")===-1 && k!='@id.sys')
                        , dataKeys = keys.filter(k => !k.endsWith(".sys"))
            let system = {}, fields = []
            sysKeys.forEach(k => system[k.replace(/.sys/,'')] = infotonL[k][0].value)
            dataKeys.sort().forEach(k => {
                var field = [k,infotonL[k]]
                for(let i=0;i<field[1].length;i++) field[1][i].hash = AppUtils.hex(field[1][i].value+field[1][i].type+(field[1][i].quad||''))
                fields.push(field)
            })
            let content = infotonL['data.content.sys'] ? infotonL['data.content.sys'][0].value : ''
            let mimeType = infotonL['mimeType.content.sys'] ? infotonL['mimeType.content.sys'][0].value : ''
            return { system, fields, content, mimeType }
        },
        gm: () => !!AppUtils.gm,
        toggleState() {
            this.sysFieldsState = (this.sysFieldsState + 1) % 3
            Settings['sysfields-state'] = this.sysFieldsState
            this.updateShownSysFields()
        },
        updateShownSysFields() {
            switch(this.sysFieldsState) {
                case 1:
                    this.shownSystem = { uuid: this.system.uuid, lastModified: this.system.lastModified }
                    break
                case 2:
                    this.shownSystem = { }
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
          let headers = AppUtils.token ? { 'X-CM-Well-Token': AppUtils.token } : { }
          fetch(this.system.path, { method: 'delete', headers }).then(() => { this.$router.push(this.system.parent) })
        }
    }
})