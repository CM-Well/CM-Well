import Vue from 'vue'
import Router from 'vue-router'
import Home from '@/views/Home.vue'
import InfotonPage from '@/views/InfotonPage.vue'

Vue.use(Router)

export default new Router({
  mode: 'history',
  base: process.env.publicPath,
  routes: [
    {
      path: '/',
      name: 'home',
      component: Home
    },
    {
      path: '/*',
      name: 'infotonPage',
      component: InfotonPage
    },
    {
      path: '/about',
      name: 'about',
      // route level code-splitting
      // this generates a separate chunk (about.[hash].js) for this route
      // which is lazy-loaded when the route is visited.
      component: () =>
        import(/* webpackChunkName: "about" */ './views/About.vue')
    }
  ]
})
