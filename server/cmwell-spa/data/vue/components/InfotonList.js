const InfotonListItem = {
    template: `<li :class="'infoton-list-item '+clazz">
                <router-link :to="path" v-on:click.native="$root.$emit('infotonItemClicked', path)">
                    {{ path | lastPartOf }}
                </router-link>
              </li>`,
    props: ['path'],
    filters: {
        lastPartOf: p => p.substring(p.lastIndexOf('/')+1)
    },
    data: () => ({ clazz: '' }),
    mounted() {
        this.$root.$on('infotonItemClicked', path => { // using a "global" event here because we need collaboration between sibling components...
            this.clazz = path == this.path ? 'selected' : ''
        })
    }
}

Vue.component('infoton-list', {
    components: { 'infoton-list-item': InfotonListItem },
    computed: {
        method() { return this.methodSelectors[this.methodSelector].method }
    },
    methods: {
        setPositionFromHeaders(response) {
            this.position = response.headers.get('X-CM-WELL-POSITION')
        },
        search(path,qp) {
            this.isLoading = true
            let params = new URLSearchParams()
            params.append('op', 'search')
            if(qp) params.append('qp', qp)
            params.append('length', 50)
            params.append('offset', this.offset)
            params.append('format', 'json')
            let url = `${path}?${params.toString()}`
            this.curAjax = url
            mfetch(url).then(resp => resp.clone().json()).then(searchResponse => {
                if(this.curAjax != url) return
                this.total = searchResponse.results.total
                this.paths = [ ...this.paths, ...searchResponse.results.infotons.map(i => i.system.path) ]
                this.exhuasted = this.paths.length == this.total
                this.isLoading = false
            })
        },     
        createConsumer(path,qp) {
            this.prevPos = this.position
            this.paths = []
            this.isLoading = true
            let params = new URLSearchParams()
            params.append('op', 'create-consumer')
            if(qp) params.append('qp', qp)
            if(this.indexTime && path!='/') params.append('index-time', this.indexTime)
            let url = `${path}?${params.toString()}`
            this.curAjax = url
            mfetch(url).then(resp => {
                if(this.curAjax != url) return
                this.setPositionFromHeaders(resp)
                this.consume()
            })
        },
        consume() {
            this.isLoading = true
            let position = this.position
            let fetchFunc = this.method == 'horizon' ? fetch : mfetch
            let url = `/_consume?position=${position}&format=text&length-hint=501`
            this.curAjax = url
            fetchFunc(url).then(resp => {
                if(this.curAjax != url) return
                this.setPositionFromHeaders(resp)
                let left = +resp.headers.get('X-CM-WELL-N-LEFT')
                this.exhuasted = left == 0
                this.total = left
                return resp.clone().text()
            }).then(paths => {
                if(paths === undefined) return
//                if(position == this.prevPos) return // scenario: while a _consume ajax was in-flight, the route has changed. we drop this response as it is irrelevant
                this.paths = [ ...this.paths, ...paths.split`\n`.filter(Boolean) ]
                this.total += this.paths.length
                this.isLoading = false
                if(this.method == 'horizon') this.timeout = setTimeout(this.consume, 5000)
            })
        },
        lastPartOf: p => p.substring(p.lastIndexOf('/')+1),
        gm: () => !!AppUtils.gm,
        toggleMethod() {
            this.methodSelector = (this.methodSelector + 1) % this.methodSelectors.length
            Settings.methodSelector = this.methodSelector
            this.reset(this.path)
        },
        togglePin() {
            this.pinned = !this.pinned
            Settings[`pin.${this.path}`] = +this.pinned
        },
        reset(path) {
            if(path === '/proc' || path === '/proc/') {
                this.$router.push('/')
                this.$nextTick(() => { this.$root.$emit('/proc') })
                return
            }
            if(path.indexOf('/proc/')===0)
                return
            
            this.path = path
            this.position = ''
            this.indexTime = 0
            this.paths = []
            this.exhuasted = false
            this.observer = null
            this.isLoading = true
            this.prevPos = ''
            this.total = ''
            this.offset = 0
            this.pinned = !!+Settings[`pin.${this.path}`]
            clearTimeout(this.timeout)
            this.purging = false,
            this.purgeProgress = ''
            
            let url = new URL(location.origin + path)
                , pathname = url.pathname
                , qp = url.searchParams.get`qp`
            
            switch(this.method) {
                case 'horizon':
                    this.indexTime = AppUtils.startTime
                case 'consume':
                    this.createConsumer(pathname, qp)
                    break
                case 'search':
                    this.search(pathname, qp)
                    break
                default:
            }            
        },
        more() {
            if(this.method == 'consume')
                this.consume()
            if(this.method == 'search') {
                this.offset += 50
                this.search(this.path)
            }
        },
        itemSelected() { if(this.pinned) this.stay = true },
        streamUuids() {
            let w = window.open(); w.document.write('<pre>')
            let handler = new ChunkedResponseHandler(chunk => {
                w.document.write(chunk.split`\n`.filter(Boolean).map(l=>l.split`\t`[2]).join`\n`)
                w.scrollTo(0,w.document.body.scrollHeight)
            })
            fetch(this.$route.path + '?op=stream&recursive&format=tsv').then(chunk => handler.process(chunk))
        },
        recursivePurge(path) {
            if(!confirm('DATA LOSS WARNING! THIS WILL PURGE ALL VERSIONS OF ALL INFOTONS UNDER THIS PATH, RECURSIVELY. AND CANNOT BE UNDONE. Are you sure you want to continue?')) return
            this.purging = true
            let headers = AppUtils.token ? { 'X-CM-Well-Token': AppUtils.token } : { }
            let purgedSoFar = 0
            fetch(path+'?op=stream&recursive').then(r=>r.text()).then(t => t.split`\n`.forEach(p => {
                fetch(p + '?op=purge-all', { headers }).then(resp => {
                    if(resp.status==200) {
                        this.purgeProgress = purgedSoFar >= this.total ? 'Completed successully.' : ++purgedSoFar + '/' + this.total
                     } else {
                         this.purgeProgress = 'An error occurred: HTTP ' + resp.status
                     }
                })
            }))
        },
        toggleTypes() {
            if(this.showTypes) {
                this.showTypes = false
                return
            }
            
            this.types = []
            this.typesAreLoading = true
            this.showTypes = true
            
            mfetch(this.$route.path + '?op=aggregate&recursive&ap=type:term,field::type.rdf,size:1024&format=json').then(resp => resp.clone().json()).then(aggrResp => {
                this.typesAreLoading = false
                if(aggrResp.AggregationResponse)
                    this.types = aggrResp.AggregationResponse[0].buckets.map(bckt => ({ uri: bckt.key, count: bckt.objects }))
            })
        }
    },
    data: () => ({ path: '', position: '', indexTime: 0, paths: [], exhuasted: false, observer: null, isLoading: true, prevPos: '', total: '', offset: 0,
                    showMenu: false, streamFormats: AppUtils.streamFormats,
                     // TODO: Consider having a MethodSelector Component that emits callback to its parent (InfotonList)
                    methodSelector: +Settings.methodSelector,
                    methodSelectors: [
                        { method: 'consume', buttonTitle: 'oldest',  titleSuffix: 'on top' },
                        { method: 'search',  buttonTitle: 'newest',  titleSuffix: 'on top' },
                        { method: 'horizon', buttonTitle: 'horizon', titleSuffix: '' }
                    ],
                    loadingIndicator: false,
                    loadingIndicatorTimeout: null,
                    showTypes: false,
                    typesAreLoading: false, // TODO: Types can be an inner component.
                    types: [],
                    pinned: false,
                    stay: false,
                    curAjax: '',
                    purging: false,
                    purgeProgress: ''
                 }),
    template: `<div class="infoton-list" v-if="path!='/'">
                    <div class="title" v-if="paths.length || method=='horizon'">
                        <span v-if="!showTypes">
                            <query-builder/>
                            <span class="hamburger" v-on:click="showMenu=!showMenu">&#9776;</span>
                            <span class="pin" v-if="pinned" title="path is pinned">&#128204;</span>
                            <span v-if="paths.length==total">{{ total }} total</span>
                            <span v-else>{{ paths.length }} of {{ total }}</span>
                            <span class="method-selector">Showing <button :disabled="isLoading" v-on:click="toggleMethod()">{{ methodSelectors[methodSelector].buttonTitle }}</button> {{ methodSelectors[methodSelector].titleSuffix }}</span>
                        </span>
                        <span v-else>Types Distribution</span>
                        <span class="types-handle" v-on:click="toggleTypes" title="toggle Types dist."><img src='data:image/svg+xml;utf8,<svg version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" height="16" fill="%23011EFF" viewBox="0 0 1792 1792" width="16"><path d="M512 896v512h-256v-512h256zm384-512v1024h-256v-1024h256zm1024 1152v128h-2048v-1536h128v1408h1920zm-640-896v768h-256v-768h256zm384-384v1152h-256v-1152h256z"/></svg>'/></span>
                    </div>
                    <msg :show="showMenu" v-on:close="showMenu=false" clazz="download-menu">
                        <div v-on:click="showMenu=false; togglePin()"><router-link to>{{ pinned ? "Unpin" : "Pin" }} {{ lastPartOf(path) }}</router-link></div>
                        <div class="hint">
                            When a path is pinned, selecting one of its Children Infotons won't fetch its own children but keep its siblings visible.
                        </div>
                        <hr/>
                        Download entire {{ lastPartOf(path) }} as:<br><ul>
                        <li v-for="format in streamFormats"><a :href="path+'?op=stream&recursive&format='+format.value" :download="lastPartOf(path)+'.'+format.value">{{ format.displayName }}</a></li>
                        </ul>
                        <span v-if="gm()" class="gm">
                            <router-link to v-on:click.native="streamUuids">Stream uuids</router-link><br/>
                            <span v-if="purging">PURGING {{ lastPartOf(path) }}/* ... {{ purgeProgress }}</span>
                            <router-link v-else to v-on:click.native="recursivePurge(path)">RECURSIVELY PURGE-ALL {{ lastPartOf(path) }}</router-link>
                        </span>
                    </msg>
                    <ul class="infotons types" v-if="showTypes">
                        
                        <li class="infoton-list-item empty-or-loading" v-if="typesAreLoading" key="loading">[ Loading types ... ]</li>
                        <li class="infoton-list-item empty-or-loading" v-if="!typesAreLoading && !types.length" key="loading">[ types not found ]</li>
                        <li class="infoton-list-item" v-for="type in types" :key="type.uri">
                            <router-link :to="'?op=search&recursive&qp=type.rdf::'+type.uri" :title="type.uri">{{ lastPartOf(type.uri) }} ({{ type.count }})</router-link>
                        </li>
                    </ul>
                    <ul class="infotons" v-else>
                        <li class="infoton-list-item empty-or-loading" v-if="!paths.length && !isLoading && method!='horizon'" key="empty"">[ no child Infotons ]</li>
                        <infoton-list-item v-for="path in paths" :key="path" :path="path" v-on:click.native="itemSelected" />
                        <li class="infoton-list-item empty-or-loading" id="observed" key="$$observed"><span v-if="isLoading">[ Loading more Infotons... ]</span>&nbsp;</li>
                        <li class="infoton-list-item empty-or-loading marginer" key="$$marginer">&nbsp;</li> 
                    </ul>
                    <div class="fader"/>
                </div>`,
    watch: {
        '$route.fullPath': function(path) { this.stay ? this.stay = false : this.reset(path) }
    },
    updated() {
        this.$nextTick(function () {
            if(!this.paths.length || this.observer)
                return

            let observerSettings = { root: document.querySelector(".infoton-list") }
            this.observer = new IntersectionObserver(entries => {
                if(entries[0].intersectionRatio > 0 && !this.exhuasted && !this.isLoading) this.more()
            }, observerSettings)
            this.observer.observe(document.querySelector("#observed"))          
        })
    }
})