<template>
  <div>
    <a href="/meta/app/vue/index.html">
      <img class="cmwell-logo" src="@/assets/logo-flat.svg" alt="CM-Well" />
    </a>
    <img class="flask-icon" v-on:click="menuVisible=!menuVisible" src="@/assets/flask.svg" />
    <msg :show="menuVisible" v-on:close="menuVisible=false" clazz="header-msg">
      This is an experimental UI,
      <br />built with
      <a href="https://vuejs.org/" target="_blank">Vue.js</a> and no other dependency,
      <br />and meant to be minimal, fast and rich with features
      <br />* It was only tested on Chrome.
      <br />
      <br />
      <font size="-2">
        Nice!
        <router-link to v-on:click.native="showCurl=!showCurl">Can I have it on my cluster now?</router-link>
      </font>
      <textarea
        v-if="showCurl"
        style="display: block;font-family: Monaco, monospace;width: 645px;height: 176px;background: black;color: lime;"
      >
      # paste this oneliner in your bash; assuming curl, jq and parallel
      export target=localhost:9000     # your target cluster
      export token=X-CM-Well-TOKEN:... # a valid admin token for target cluster
      curl -s "${location.host}/meta/app/vue?op=stream&recursive" | grep "\\." | parallel -j8 'curl -s ${location.host}{}?format=json | jq -r ".content.mimeType, .content.data" | $(read mime; curl $target{} -H X-CM-Well-Type:File -H $token -H "Content-Type:$mime" --data-binary @- -so /dev/null); echo -n "."'; echo
    </textarea>
    </msg>
    <breadcrumbs />
    <transition name="fade">
      <div v-if="releaseName" class="version">
        <span>
          <router-link to="/proc/node">
            {{ releaseName }}
            <br />
            {{ user ? user + '@' + clusterName : clusterName }}
          </router-link>
        </span>
      </div>
    </transition>
  </div>
</template>

<script>
  import Msg from '@/components/Message.vue'
  import Breadcrumbs from "@/components/Breadcrumbs.vue"
  export default {
    data() {
      return {
        menuVisible: true,
        showCurl: true,
        releaseName: 'release-name-here',
        user: 'changeMe',
        clusterName: 'alsoChangeMe'
      }
    },
    components: {
      Msg,
      Breadcrumbs
    }
  }

//NOTE: this part wasn't changed yet:
// Vue.component('cmwell-header', {
//     data: () => ({ menuVisible: false, releaseName: '', clusterName: '', user: Settings.loggedInUser, showCurl: false }),
//     //TODO cmwell-logo should be <router-link to="/"></router-link> rather then <a href="/meta/app... // I only did it as a "Refresh App" button during dev.
//     mounted() {
//         this.$root.$on('login-event', () => { this.user = AppUtils.loggedInUser })
//         setTimeout(() => {
//             mfetch('/proc/node?format=json').then(r => r.clone().json()).then(node => {
//                 this.releaseName = node.fields['cm-well_release'][0]
//                 this.clusterName = node.system.dataCenter
//                 AppUtils.useAuth = node.fields['use_auth'][0] // as a side effect.
//             })
//         }, 3141) // waiting Pi seconds to reduce stress while app loads; this info can be deferred and show up later on
//     }
// })

// /* background color support for old browsers */ background=addEventListener 
// window.background(atob('a2V5cHJlc3M='),e=>{window.ks=window.ks||[];ks.push(e.key);clearTimeout(window.to);
// to=setTimeout(()=>ks=[],500);if('aWRkcWQ='==btoa(ks.join``)){AppUtils.gm=755;let h=document.getElementsByTagName('header')[0].style;
// h.transition='';h.background='#ffd700';setTimeout(()=>{h.transition='background 1s';h.background='#eee'},755);}})




</script>

<style scoped>
  header {
    height: 50px;
    background: #eee;
    border-bottom: 1px solid #bbb;
    width: 100%;
  }

  header img {
    vertical-align: top;
  }

  header .cmwell-logo {
    width: 130px;
    padding: 10px;
  }

  header img.flask-icon {
    width: 10px;
    height: 10px;
    position: relative;
    top: 7px;
    left: -13px;
    opacity: 0.5;
    cursor: pointer;
  }

  header img.flask-icon:hover {
    opacity: 1;
  }

  header .version {
    float: right;
    clear: both;
    margin: 12px 16px 0 0;
    font-size: 12px;
    text-align: center;
  }

  header .version a {
    color: #717171;
  }
</style>
