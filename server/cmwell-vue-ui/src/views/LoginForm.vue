<template>
  <div class="login-form">
    <h2>Login</h2>
    <div class="hint">
      Logging in allows you to modify data from this UI.
      <br />
      This cluster {{ useAuth ? 'requires' : 'does not require' }} authentication.
    </div>
    <span v-if="loggedInUser && !renewal">
      <div class="info">
        You are already logged in as {{ loggedInUser }}
        <br />
        Write Token will expire on {{ expired }}
        <br />
        <pre v-if="displayToken">{{ existingToken }}</pre>
        <span v-else>
          <router-link to v-on:click.native="displayToken=true">View token</router-link>
        </span>
        <br />
      </div>
      <router-link to v-on:click.native="close">Dismiss</router-link>&nbsp;&nbsp;&nbsp;
      <router-link to v-on:click.native="renew">Renew</router-link>&nbsp;&nbsp;&nbsp;
      <router-link to v-on:click.native="clear">Forget me from this device</router-link>
    </span>
    <span v-else>
      <span v-if="!tokenMode">
        <input type="text" v-model="username" @keyup.enter="login" @focus="error=''" />
        <span class="label">username</span>
        <br />
        <input type="password" v-model="password" @keyup.enter="login" @focus="error=''" />
        <span class="label">password</span>
        <br />
        <div class="info-center">
          Alternatively,
          <router-link to v-on:click.native="tokenMode=true">use token</router-link>.
        </div>
      </span>
      <span v-else>
        <input type="text" v-model="token" @keyup.enter="login" @focus="error=''" />
        <span class="label">token</span>
        <br />
        <router-link to v-on:click.native="tokenMode=false">use credentials instead</router-link>
        <br />
        <br />
      </span>

      <label>
        <input type="checkbox" v-model="persist" />Remember me on this device
      </label>
      <br />

      <center>
        <button v-on:click="close">Cancel</button>
        <span v-if="loading">loading...</span>
        <button
          v-else
          v-on:click="login"
          :disabled="!(tokenMode && token) && !(!tokenMode && username && password)"
        >
          <b>Login</b>
        </button>
        <span v-if="msg">
          <br />
          {{ msg }}
        </span>
      </center>
      <span class="error" v-if="error">{{ error }}</span>
    </span>
  </div>
</template>

<script>
  export default {
    data() {
      return {
        tokenMode: false,
        loading: false,
        error: '',
        username: '',
        password: '',
        token: '',
        persist: false,
        msg: '',
        renewal: false,
        displayToken: false
      }
    },
    computed: {
      useAuth() {
        return AppUtils.useAuth
      },
      loggedInUser() {
        return Settings.loggedInUser || AppUtils.loggedInUser
      },
      existingToken() {
        return Settings.token || AppUtils.token
      },
      expired() {
        let tok = Settings.token || AppUtils.token
        return tok
          ? new Date(JSON.parse(atob(tok.split`.`[1])).exp).toLocaleDateString()
          : ''
      }
    },
    methods: {
      login() {
        if (this.token) {
          this.setToken()
        } else {
          this.error = ''
          this.loading = true
          fetch('/_login?exp=7d', {
            headers: {
              Authorization: `Basic ${btoa(`${this.username}:${this.password}`)}`
            }
          }).then(resp => {
            this.loading = false
            if (resp.status === 200) {
              resp.json().then(loginResult => {
                AppUtils.token = loginResult.token
                AppUtils.loggedInUser = this.username
                this.setTokenCookie(loginResult.token)
                if (this.persist) {
                  Settings.token = loginResult.token
                  SettinLogings.loggedInUser = this.username
                }
                this.$root.$emit('login-event')
                this.msg = 'Authenticated successfully.'
                setTimeout(this.close, 1024)
              })
            } else {
              resp
                .text()
                .then(
                  loginResult =>
                    (this.error = `HTTP ${resp.status}: ${loginResult}`)
                )
            }
          })
        }
      },
      close() {
        this.tokenMode = this.loading = this.persist = this.renewal = this.displayToken = false
        this.error = this.username = this.password = this.token = this.msg = ''
        this.$emit('closeLoginForm')
      },
      renew() {
        this.username = AppUtils.loggedInUser
        Settings.token = Settings.loggedInUser = AppUtils.token = AppUtils.loggedInUser =
          ''
        this.tokenMode = this.loading = this.persist = this.displayToken = false
        this.error = this.password = this.token = this.msg = ''
        this.renewal = true
      },
      clear() {
        Settings.token = Settings.loggedInUser = AppUtils.token = AppUtils.loggedInUser =
          ''
        this.removeTokenCookie()
        this.$root.$emit('login-event')
        this.close()
      },
      setToken() {
        if (!this.token) return
        var claims = {}
        try {
          claims = JSON.parse(atob(this.token.split`.`[1]))
        } catch (e) {
          this.error = 'Given token is malformed.'
          return
        }
        AppUtils.token = this.token
        AppUtils.loggedInUser = claims.sub
        this.setTokenCookie(this.token)
        if (this.persist) {
          Settings.token = this.token
          Settings.loggedInUser = claims.sub
        }
        this.close()
      },
      setTokenCookie(token) {
        document.cookie =
          `X-CM-WELL-TOKEN=${token}; path=/;` +
          (this.persist
            ? ` expires=${new Date(+new Date() + 7 * 24 * 3600 * 1000)}`
            : '')
      },
      removeTokenCookie() {
        document.cookie = `X-CM-WELL-TOKEN=; path=/; expires=${new Date(0)};`
      }
    }
  }
</script>

<style>
</style>
