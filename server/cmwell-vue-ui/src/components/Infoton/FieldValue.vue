<template>
  <div>
    <a v-if="this.isURL && !this.isInternal" :href="value.value" target="_blank">
      {{ value.value }}
      <img
      class="link-out"
      src="data:image/svg+xml;utf8,
      <svg height="1024" width="768" xmlns="http://www.w3.org/2000/svg">
        <path
          d="M640 768H128V257.90599999999995L256 256V128H0v768h768V576H640V768zM384 128l128 128L320 448l128 128 192-192 128 128V128H384z"
        />
      </svg>"
      />
    </a>
    <router-link v-if="this.isURL && this.isInternal" :to="value.value | asPath">{{ value.value }}</router-link>
    <span v-if="!this.isURL">{{ value.value }}</span>
    <span class="quad" v-if="value.quad" :title="value.quad">
      <a
        :href="'/?op=search&recursive&qp=system.quad::' + value.quad"
        target="_blank"
      >{{ value.quad | lastPartOf }}</a>
    </span>
  </div>
</template>

<script>
export default {
  // todo quad icon
  data() {
    return {
      isURL: this.value.type === 'http://www.w3.org/2001/XMLSchema#anyURI',
      isInternal: false
    }
  },
  props: ['value', 'quadFilter'],
  beforeMount() {
    if (this.isURL)
      AppUtils.topLevelDomains.then(tlds => {
        let path = AppUtils.pathFromURL(this.value.value),
          domain = path.substring(1, path.indexOf('/', 1))
        this.isInternal = tlds.includes(domain)
      })
  },
  filters: {
    asPath: AppUtils.pathFromURL,
    lastPartOf: p => p.substring(p.lastIndexOf('/') + 1)
  }
}
</script>

<style>
</style>
