<template>
  <div>
    <span v-if="collapsed" class="qb-btn">
      <router-link to v-on:click.native="loadMetaNs(); collapsed=false">&#128269;</router-link>
    </span>

    <div v-else class="qb-container">
      <span v-on:click="collapsed=true" class="close-button">X</span>
      <h2>Query Builder</h2>

      <div :class="'the-query'+(isValid?'':' invalid')">?{{ query }}</div>

      <div
        v-on:click="qpVisible=!qpVisible"
        class="plus-btn"
        title="Add QP"
      >{{ qpVisible? '-' : '+' }}</div>
      <label>add QP</label>
      <div v-if="qpVisible" class="qp-section">
        <selector-group
          v-for="g in groups"
          :key="g"
          :groupId="g"
          v-on:changed="changed"
          v-on:remove="removeGroup"
          :fields="fields"
        />
        <div v-on:click="add" class="plus-btn">+</div>
      </div>

      <br />
      <div
        v-on:click="xgVisible=!xgVisible; updateQuery(this.isValid)"
        class="plus-btn"
        title="Add XG"
      >{{ xgVisible? '-' : '+' }}</div>
      <label>add XG</label>
      <div v-if="xgVisible" class="input-section">
        <input
          v-model="xgString"
          @change="updateQuery(this.isValid)"
          placeholder="enter the xg string"
        />
      </div>

      <br />
      <div
        v-on:click="ygVisible=!ygVisible; updateQuery(this.isValid)"
        class="plus-btn"
        title="Add YG"
      >{{ ygVisible? '-' : '+' }}</div>
      <label>add YG</label>
      <div v-if="ygVisible" class="input-section">
        <input
          v-model="ygString"
          @change="updateQuery(this.isValid)"
          placeholder="enter the yg string"
        />
      </div>

      <br />
      <div
        v-on:click="gqpVisible=!gqpVisible; updateQuery(this.isValid)"
        class="plus-btn"
        title="Add GQP"
      >{{ gqpVisible? '-' : '+' }}</div>
      <label>add GQP</label>
      <div v-if="gqpVisible" class="input-section">
        <input
          v-model="gqpString"
          @change="updateQuery(this.isValid)"
          placeholder="enter the gqp string"
        />
      </div>

      <input type="checkbox" id="checkbox" v-model="recursive" />
      <label for="checkbox">recursive?</label>

      <router-link
        tag="button"
        class="control"
        id="button"
        :to="$route.path + '?qp=' + query"
        style="float: right"
      >Apply</router-link>
    </div>
  </div>
</template>

<script>
  export default {
    data() {
      return {
        collapsed: true,
        qpVisible: false,
        xgVisible: false,
        xgString: '',
        ygVisible: false,
        ygString: '',
        gqpVisible: false,
        gqpString: '',
        namespaces: {},
        fields: [],
        groups: [1],
        nextGroupId: 2,
        queryParts: [],
        query: '',
        isValid: true,
        recursive: false
      }
    },
    components: { 'selector-group': SelectorGroup, autocomplete: AutoComplete },
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
        var qpPart = this.queryParts
          .filter(Boolean)
          .map(i => {
            let { quantifier, not, groupQuery } = i
            let wrap = quantifier === 'should' && not
            let prefix = wrap
              ? '*[-['
              : (not ? '-' : '') + (quantifier === 'should' ? '*[' : '[')
            let suffix = wrap ? ']]' : ']'
            return prefix + groupQuery + suffix
          })
          .join(',')
        qpPart = qpPart.length > 0 ? 'qp=' + qpPart : ''
        var xgPart =
          this.xgVisible && this.xgString.length > 0 ? 'xg=' + this.xgString : ''
        xgPart = qpPart.length > 0 && xgPart.length > 0 ? '&' + xgPart : xgPart
        var ygPart =
          this.ygVisible && this.ygString.length > 0 ? 'yg=' + this.ygString : ''
        ygPart =
          qpPart.length + xgPart.length > 0 && ygPart.length > 0
            ? '&' + ygPart
            : ygPart
        var gqpPart =
          this.gqpVisible && this.gqpString.length > 0
            ? 'gqp=' + this.gqpString
            : ''
        gqpPart =
          qpPart.length + xgPart.length + ygPart.length > 0 && gqpPart.length > 0
            ? '&' + gqpPart
            : gqpPart

        this.query =
          qpPart +
          xgPart +
          ygPart +
          gqpPart +
          (this.recursive ? '&recursive' : '')
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

        if (this.fields.length) return
        let r = new RegExp(`<${location.origin}/meta/ns/(.*)>`)
        let parseNsNtriple = ntriple => {
          if (!ntriple) return
          let parts = ntriple.split` `
          try {
            return [r.exec(parts[0])[1], parts[2].replace(/"/g, '')]
          } catch (_) {}
        }
        let parseFieldPath = path => {
          if (!path) return
          try {
            return path.replace('/meta/ns/', '').split`/`
          } catch (_) {}
        }
        mfetch('/meta/ns?op=stream&format=ntriples&fields=prefix')
          .then(r => r.clone().text())
          .then(t => {
            this.namespaces = new BiMap(
              t.split`\n`.map(parseNsNtriple).filter(Boolean)
            )
            mfetch('/meta/ns?op=stream&recursive&format=text')
              .then(r => r.clone().text())
              .then(t => {
                this.fields = t.split`\n`
                  .map(parseFieldPath)
                  .filter(prts => prts && prts.length === 2)
                  .map(ltp => `${ltp[1]}.${this.namespaces[ltp[0]]}`)
              })
              .catch(e => {
                console.warn('Could not load meta/ns! error was:', e)
              })
          })
          .catch(e => {
            console.warn('Could not load meta/ns! error was:', e)
          })
      }
    }
  }
</script>

<style>
</style>
