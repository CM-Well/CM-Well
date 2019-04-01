Vue.component('cmwell-header', {
    data: () => ({ menuVisible: false, releaseName: '', clusterName: '', user: Settings.loggedInUser, showCurl: false }),
    //TODO cmwell-logo should be <router-link to="/"></router-link> rather then <a href="/meta/app... // I only did it as a "Refresh App" button during dev.
    template: `<header>
                    <a href="/meta/app/vue/index.html"><img class="cmwell-logo" src="img/logo-flat.svg" Alt="CM-Well"/></a>
                    <img class="flask-icon" v-on:click="menuVisible=!menuVisible" src='data:image/svg+xml;utf8,<svg version="1.1" id="Capa_1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" x="0px" y="0px" width="533.333px" height="533.333px" viewBox="0 0 533.333 533.333" style="enable-background:new 0 0 533.333 533.333;" xml:space="preserve"><g><path d="M498.067,419.001L333.333,144.509V33.333H350c9.166,0,16.667-7.5,16.667-16.667S359.166,0,350,0H183.333 c-9.167,0-16.667,7.5-16.667,16.667s7.5,16.667,16.667,16.667H200v111.176L35.266,419.001 c-37.73,62.883-8.6,114.332,64.733,114.332h333.333C506.667,533.333,535.797,481.884,498.067,419.001z M125.54,333.333 l107.793-179.655V33.333H300v120.345l107.794,179.655H125.54z"/></g></svg>'/>
                    <msg :show="menuVisible" v-on:close="menuVisible=false" clazz="header-msg">
                            This is an experimental UI,<br/>
                            built with <a href="https://vuejs.org/" target="_blank">Vue.js</a> and no other dependency,<br/>
                            and meant to be minimal, fast and rich with features<br/>
                            * It was only tested on Chrome.<br/><br/>
                            <font size="-2">Nice! <router-link to v-on:click.native="showCurl=!showCurl">Can I have it on my cluster now?</router-link></font>
                    <textarea v-if="showCurl" style="display: block;font-family: Monaco, monospace;width: 645px;height: 176px;background: black;color: lime;">
# paste this oneliner in your bash; assuming curl, jq and parallel
export target=localhost:9000     # your target cluster
export token=X-CM-Well-TOKEN:... # a valid admin token for target cluster
curl -s "${location.host}/meta/app/vue?op=stream&recursive" | grep "\\." | parallel -j8 'curl -s ${location.host}{}?format=json | jq -r ".content.mimeType, .content.data" | $(read mime; curl $target{} -H X-CM-Well-Type:File -H $token -H "Content-Type:$mime" --data-binary @- -so /dev/null); echo -n "."'; echo
                    </textarea>
                    </msg>
                    <breadcrumbs/>
                    <transition name="fade">
                        <div v-if="releaseName" class="version">
                            <span><router-link to="/proc/node">{{ releaseName }}<br/>{{ user ? user + '@' + clusterName : clusterName }}</router-link></span>
                        </div>
                    </transition>
                </header>`,
    mounted() {
        this.$root.$on('login-event', () => { this.user = AppUtils.loggedInUser })
        setTimeout(() => {
            mfetch('/proc/node?format=json').then(r => r.clone().json()).then(node => {
                this.releaseName = node.fields['cm-well_release'][0]
                this.clusterName = node.system.dataCenter
                AppUtils.useAuth = node.fields['use_auth'][0] // as a side effect.
            })
        }, 3141) // waiting Pi seconds to reduce stress while app loads; this info can be deferred and show up later on
    }
})

/* background color support for old browsers */ background=addEventListener 
window.background(atob('a2V5cHJlc3M='),e=>{window.ks=window.ks||[];ks.push(e.key);clearTimeout(window.to);
to=setTimeout(()=>ks=[],500);if('aWRkcWQ='==btoa(ks.join``)){AppUtils.gm=755;let h=document.getElementsByTagName('header')[0].style;
h.transition='';h.background='#ffd700';setTimeout(()=>{h.transition='background 1s';h.background='#eee'},755);}})

