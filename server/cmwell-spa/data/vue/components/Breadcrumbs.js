Vue.component('breadcrumbs', {
    data: () => ({ crumbs: [], isScrolled: false }),
    template: `<div class="breadcrumbs">
                    <span v-if="isScrolled" class="fader">&nbsp;</span>
                    <span v-if="crumbs.length">/</span>
                    <span v-for="crumb in crumbs" class="crumb"><router-link :to="crumb.path" :key="crumb.path">{{ crumb.title }}</router-link></span>
            </div>`,
    watch: { '$route.fullPath': function(path) {
        this.crumbs = [ ]
        this.isScrolled = false
        let parts = this.$route.path.split`/`.splice(1).filter(Boolean) // TODO SearchParams. Suggested impl.: parts (with slashes) + magnifier glass icon + search criteria
        
        if(!parts.length)
            return

        let pathBuilder = ""
        parts.forEach(part => {
            pathBuilder += "/" + part
            this.crumbs.push({ path: pathBuilder, title: part })
        })
        
        this.$nextTick(function() {
            this.$el.scrollLeft = this.$el.scrollWidth
            this.isScrolled = !!this.$el.scrollLeft
        })
    } }
})