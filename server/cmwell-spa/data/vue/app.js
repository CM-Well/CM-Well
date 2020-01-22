window.AppUtils = {
    memoizie(task, rejectBy) {
        let cache = { }, capacity = 1024
        return k => {
            if(Object.keys(cache).length >= capacity) cache = { }
            if(cache[k]) return cache[k]
            cache[k] = task(k)
            if(rejectBy) cache[k].then(v => { if(rejectBy(v)) delete cache[k] })
            return cache[k]
        }
    },
    pathFromURL: url => url.replace(/http(s)?:\//,''),
    hex(str) { return str.split``.map(c=>c.charCodeAt().toString(16)).join`` },
    formats: [
        { value: 'jsonld&pretty', displayName: 'JSON-LD' },
        { value: 'n3', displayName: 'N3' },
        { value: 'ntriples', displayName: 'NTriples' },
        { value: 'nquads', displayName: 'NQuads' },
        { value: 'trig', displayName: 'TriG' },
        { value: 'trix', displayName: 'TriX' },
        { value: 'turtle', displayName: 'Turtle' },
        { value: 'rdfxml', displayName: 'RDF-XML' }
    ],
    streamFormats: [
        { value: 'text', displayName: 'Just paths' },
        { value: 'tsv', displayName: 'TSV' },
        { value: 'json', displayName: 'JSON Seq' },
        { value: 'ntriples', displayName: 'NTriples' },
        { value: 'nquads', displayName: 'NQuads' }
    ],
    startTime: +new Date,
    topLevelDomains: [ ]
}

// BiMap: Immutable BiDirectional Map impl. using twice memory,
// in favor of returning value in O(1)
class BiMap {
    constructor(pairs) {
        this._fw = {}
        this._bw = {}
        pairs.forEach(p => {
            this._fw[p[0]] = p[1]
            this._bw[p[1]] = p[0]
        })
        return new Proxy(this, {
            get: (_, prop) => {
                if(prop==='_fw') return this._fw
                if(prop==='_bw') return this._bw
                return this._fw[prop] || this._bw[prop]
            },
            set() { throw new Error("Illegal operation") }
        })
    }
}

class ChunkedResponseHandler {
    constructor(handler) {
        this.handler = handler
        this.decoder = new TextDecoder()
    }
    
    process(resp) {
        this.readChunk(resp.body.getReader())
    }
    
    readChunk(reader) {
        reader.read().then(buffer => {
            let decodedChunk = this.decoder.decode(buffer.value || new Uint8Array, { stream: !buffer.done })
            this.handler(decodedChunk)
            if(!buffer.done) this.readChunk(reader)
        })
    }
}

// mfetch:
// 1. is needed until fetch(..., { cache: 'force-cache' }) is supported in browsers
// 2. USAGE NOTE: in order to allow body stream consumption more than once, please .clone() before .text(), .blob() or .json()
window.mfetch = AppUtils.memoizie(fetch, resp => ![200,204].includes(resp.status))

// localStorge syntactic sugar:
window.Settings = new Proxy({}, { get: (_,k) => localStorage.getItem(k), set: (_,k,v) => localStorage.setItem(k,v) })

// native sort syntactic sugar:
Array.prototype.sortBy = function(predicate) { return this.sort((el1,el2) => predicate(el1) - predicate(el2)) } // assuming numerial comparison (not lexical!)

let components = ['Header','InfotonList','Infoton','Msg','Breadcrumbs','Traversal', 'HomePage','QueryBuilder'].map(comp => `components/${comp}.js`)
load(components)(()=>{

const RouterViewContainer = {
    template: `<div class="main-container">
                <cmwell-header/>
                <home-page v-if="$route.fullPath=='/'" />
                <div class="flexy-container">
                    <infoton-list/>
                    <infoton/>
                </div>
                <traversal/>
              </div>`
}
    
const routes = [
    { path: '/*', component: RouterViewContainer }
]
const router = new VueRouter({ routes, mode: 'history' })
new Vue({ router }).$mount('#app')

router.push('/')
    
})



