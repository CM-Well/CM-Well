const AutoComplete = {
    // Using HTML5's <datalist> is pretty straightforward, until the moment you're facing >5000 elements in DOM...
    props: ['fields', 'placeholder'],
    data: () => ({ field: '', activeFields: [] }),
    template: `
                <div class="autocomplate-container">
                    <input type="text" v-model="field" :placeholder="placeholder" v-on:keyup="filterFields" v-on:change="$emit('input',field)" /><br/>
                    <ul class="autocomplete">
                        <li v-for="f in activeFields" v-on:click="field=f;activeFields=[];$emit('input',f)">{{ f }}</li>
                    </ul>
                </div>
              `,
    methods: {
        filterFields() {
            this.activeFields = this.field ? this.fields.filter(f => f.toLowerCase().indexOf(this.field.toLowerCase()) != -1) : []
        },
    }
}

const FieldSelector = {
    components: { 'autocomplete': AutoComplete },
    props: ['selId', 'fields'],
    data: () => ({ quantifier: 'must', not: false, field: '', op: '::', fvalue: '' }),
    template: `<div class="field-selector">
                    <span v-on:click="remove" class="plus-btn" title="remove">-</span>
                    <select v-model="quantifier" v-on:change="updateParent">
                        <option value="must" selected>MUST</option>
                        <option value="should">SHOULD</option>
                    </select>
                    <label><input v-model="not" type="checkbox" v-on:change="updateParent"/>not</label>
                    <autocomplete v-model="field" :fields="fields" placeholder="field name" v-on:input="updateParent" />
                    <select v-model="op" v-on:change="updateParent">
                        <option value="::" selected>Exact Match</option>
                        <option value=":">Containts</option>
                        <option value=">>">&gt;</option>
                        <option value=">">&gt;=</option>
                        <option value="<<">&lt;</option>
                        <option value="<">&lt;=</option>
                    </select>
                    <input v-model="fvalue" type="text" class="fvalue" v-on:change="updateParent" placeholder="value"/>
               </div>`,
    methods: {
        updateParent() {
            let wrap = this.quantifier == 'should' && this.not
            let toString = `${this.quantifier === 'should' ? '*' : ''}${wrap ?'[':''}${this.not ? '-' : ''}${this.field}${this.op}${this.fvalue}${wrap?']':''}`
            let isValid = this.field && (this.fvalue || this.op===':')
            this.$emit('changed', this.selId, isValid, toString)
        },
        remove() {
            this.$emit('remove', this.selId)
        }
    }
}

const SelectorGroup = {
    components: { 'field-selector': FieldSelector },
    props: ['groupId', 'fields'],
    data: () => ({
        quantifier: 'must',
        not: false,
        selectors: [1],
        nextSelId: 2,
        query: []
    }),
    template: `<div class="group-container">
                    <div class="group-meta">
                        <span v-on:click="remove" class="plus-btn" title="remove">-</span>
                        <select v-model="quantifier" v-on:change="changed">
                            <option value="must" selected>MUST</option>
                            <option value="should">SHOULD</option>
                        </select>
                        <label><input v-model="not" type="checkbox" v-on:change="changed"/>not</label>
                    </div>
                    <div class="group">
                        <field-selector v-for="s in selectors" :key="s" :selId="s" v-on:changed="changed" v-on:remove="removeSelector" :fields="fields" />
                        <div v-on:click="add" class="plus-btn">+</div>
                    </div>
               </div>`,
    methods: {
        add() {
          this.selectors = [...this.selectors, this.nextSelId++]
        },
        changed(id, isValid, fVal) {
          this.query[id] = fVal
          this.$emit('changed', this.groupId, isValid, this.quantifier, this.not, this.query.filter(Boolean).join(','))
        },
        removeSelector(id) {
          this.selectors = this.selectors.filter(s => s != id)
          this.query[id] = null
        },
        remove() {
            this.$emit('remove', this.groupId)
        }
    }
}

Vue.component('query-builder', {
    components: { 'selector-group': SelectorGroup, 'autocomplete': AutoComplete },
    data: () => ({
        collapsed: true,
        namespaces: {},
        fields: [],
        groups: [1],
        nextGroupId: 2,
        queryParts: [],
        query: '',
        isValid: true,
        recursive: false,
    }),
    template: `
                <span v-if="collapsed" class="qb-btn">
                    <router-link to v-on:click.native="loadMetaNs(); collapsed=false">&#128269;</router-link>
                </span>
                

                <div v-else class="qb-container">

                    <span v-on:click="collapsed=true" class="close-button">X</span>
                    <h2>Query Builder</h2>

                    <div :class="'the-query'+(isValid?'':' invalid')">
                        ?qp={{ query }}
                    </div>

                    <input type="checkbox" id="checkbox" v-model="recursive">
                    <label for="checkbox"> recursive? </label>

                    <selector-group v-for="g in groups" :key="g" :groupId="g" v-on:changed="changed" v-on:remove="removeGroup" :fields="fields" />
                    <div v-on:click="add" class="plus-btn">+</div>


                    <router-link tag="button" class="control" id="button" :to="$route.path + '?qp=' + query" style="float: right">Apply</router-link>
                </div>
              `,
    methods: {
      add() {
          this.groups = [...this.groups, this.nextGroupId++]
          console.log(this.nextGroupId)
      },
      removeGroup(id) {
          this.groups = this.groups.filter(s => s != id)
          this.queryParts[id] = null
          this.updateQuery()
      },
      changed(groupId, isValid, quantifier, not, groupQuery) {
          this.queryParts[groupId] = { quantifier, not, groupQuery }
          this.updateQuery(isValid)
      },
      updateQuery(isValid) {
          this.query = this.queryParts.filter(Boolean).map(i => {
              let { quantifier, not, groupQuery } = i
              let wrap = quantifier === 'should' && not
              let prefix = wrap ? '*[-[' : (not ? '-' : '') + (quantifier === 'should' ? '*[' : '[')
              let suffix = wrap ? ']]' : ']'
              return prefix + groupQuery + suffix
          }).join(',') + ((this.recursive) ? '&recursive' : '')
          this.isValid = isValid
      },
      loadMetaNs() {
          
        // using format=ntriples without "recursive" to have a BiMap(hash<->prefix)
        // using format=text with "recursive" to get all fields and matching them with their parent's prefix
        // Note: /proc/fields won't help here. We want "type.rdf", not "type.lzN1FA"...

        // each entry parsing is wrapped in try-catch to be safe from bad entries
        // and the whole process is guarded with Promise.catch as well
          
        // potential improvement: get mang info as well (simply by using json rather than text),
        //                        and expose it in UI (at least as Numerical/Textual/DateTime).
          
        if(this.fields.length) return
        let r = new RegExp(`<${location.origin}/meta/ns/(.*)>`)
        let parseNsNtriple = ntriple => {
            if(!ntriple) return
            let parts = ntriple.split` `
            try { return [ r.exec(parts[0])[1] , parts[2].replace(/"/g,'') ] } catch(_) { }
        }
        let parseFieldPath = path => { if(!path) return; try { return path.replace('/meta/ns/','').split`/` } catch(_) { } }
        mfetch('/meta/ns?op=stream&format=ntriples&fields=prefix').then(r=>r.clone().text()).then(t => {
            this.namespaces = new BiMap(t.split`\n`.map(parseNsNtriple).filter(Boolean))
            mfetch('/meta/ns?op=stream&recursive&format=text').then(r => r.clone().text()).then(t => {
                this.fields = t.split`\n`.map(parseFieldPath)
                                         .filter(prts => prts && prts.length===2)
                                         .map(ltp => `${ltp[1]}.${this.namespaces[ltp[0]]}`)
            }).catch( e => { console.warn("Could not load meta/ns! error was:", e) })
        }).catch(e => { console.warn("Could not load meta/ns! error was:", e) })          
      }  
    }
})