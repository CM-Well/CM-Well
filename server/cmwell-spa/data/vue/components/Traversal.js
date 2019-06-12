Vue.component('traversal', {
    data: () => ({ graph: [{ path: "www.google.com", title: "TMS" , relationName: "City"}, { path: "www.google.com", title: "PT" , relationName: "City"}], 
        isScrolled: false,
        shouldResetGraph: true }),
    template: `<div class="traversal">
                    <span v-if="isScrolled" class="fader">&nbsp;</span>
                    <span v-if="graph.length">/</span>
                    <span v-for="infoton in graph" class="infoton">
                        <router-link :to="infoton.path | asPath" :key="infoton.path">{{ infoton.title }}</router-link>
                        <i class="fa fa-angle-double-right" data-toggle="tooltip" :title="infoton.relationName" ></i>
                    </span>
            </div>`,
    mounted() {
        this.$root.$on('addToTraversal', (path, title, relationName) => { // using a "global" event here because we need collaboration between sibling components...
            console.log("in addToTraversal")
            this.shouldResetGraph = false
            this.addToGraph(path, title, relationName)
        })
    },
    filters: {
        asPath: AppUtils.pathFromURL
    },
    watch: {
        '$route.fullPath': function (path) { console.log("in watch") 
            if (this.shouldResetGraph)
                this.resetGraph()
            this.shouldResetGraph = true    
        }
    },
    methods: {
        resetGraph(){
            console.log("Going to reset graph")
            this.graph = [{ path: "www.google.com", title: "TMS", relationName: "City"}]
        },
        addToGraph(path, title, relationName){
            console.log("Will not reset graph")
            console.log(relationName)
            console.log(path)
            this.graph.push({ path: path, title: title, relationName: relationName })
        }
    }
})