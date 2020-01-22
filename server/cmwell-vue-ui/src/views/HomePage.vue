<template>
  <div class="homepage-container">
    <login-form v-if="showLoginForm" v-on:closeLoginForm="showLoginForm=false" />

    <div class="home system" v-if="!showLoginForm">
      <router-link
        to
        v-on:click.native="showDashboards=!showDashboards"
      >{{ showDashboards ? 'Hide ' : '' }}System Info</router-link>
      <router-link to="/meta">/meta</router-link>
      <router-link to v-on:click.native="showLoginForm=true">Log In</router-link>
      <router-link to>
        SPARQL
        <br />(coming soon)
      </router-link>
    </div>

    <div class="home" v-if="!showLoginForm && !showDashboards">
      <span v-if="loadingTlds" class="loading">Loading exciting content...</span>
      <router-link v-for="domain in tlds" :key="domain" :to="'/'+domain">{{ domain }}</router-link>
    </div>

    <div class="home" v-if="!showLoginForm && showDashboards">
      <router-link to="/proc/node" key="node">node</router-link>
      <a
        v-for="dashboard in dashboards"
        :key="dashboard"
        :href="'/proc/'+dashboard"
        target="_blank"
      >{{ dashboard | firstName }}</a>
    </div>

    <div style="margin-right:128px">
      <ul>
        <li>//todo: Types; support qp in InfotonList</li>
        <li>
          //todo: graph traversal suggestions. Layout: As a right drawer. progress will be top-down, like this:
          <pre>

sub1
 |
p1
 |
 v
su2b
 |
p2
 |
 v
sub3 &lt;-- you are here

</pre>
        </li>
        <li>//todo: qb: add graphTraversal part.</li>
        <li>//todo: qb: add Commit button</li>
        <li>//todo: qb: reverse binding + management (save,load) - not a blocker</li>
      </ul>
    </div>
  </div>
</template>

<script>
import LoginForm from "@/views/LoginForm.vue";
  export default {
    data() {
      return {
        tlds: (Settings.tlds || '').split`;`.filter(Boolean),
        loadingTlds: !Settings.tlds,
        dashboards: [
          'bg.md',
          'health-detailed.md',
          'search-contexts.md',
          'members-all.csv',
          'singletons.md',
          'requests.md',
          'dc-health.md',
          'dc-distribution.md',
          'stp.md'
        ],
        showLoginForm: false,
        showDashboards: false
      }
    },
    components: {
      'login-form': LoginForm
    },
    mounted() {
      this.$root.$on('/proc', () => {
        this.showDashboards = true
      })

      AppUtils.topLevelDomains = mfetch('/?op=stream')
        .then(r => r.clone().text())
        .then(r =>
          r.trim().split`\n`
            .map(p => p.substring(1))
            .filter(p => p != 'meta' && p != 'blank_node')
        )
      AppUtils.topLevelDomains.then(tlds => {
        this.loadingTlds = false
        this.tlds = tlds.sort()
        Settings.tlds = tlds.join`;`

        // So, what was that? We keep the TopLevelDomains in localStorage ("Settings" is a syntatic sugar).
        // Next time the app starts, if connection is poor, user will have a (perhaps not up-to-date) list of those ready for them.
        // Still we fire that ajax and update localStorage with more up-to-date list.

        // We keep the tlds in the AppUtils scope, so it will be available for FieldValue as well, to distinguish inner links from outer links.
      })
    },
    filters: {
      firstName(filename) {
        let idx = filename.lastIndexOf('.')
        return idx === -1 ? filename : filename.substring(0, idx)
      }
    }
  }
</script>

<style>
</style>
