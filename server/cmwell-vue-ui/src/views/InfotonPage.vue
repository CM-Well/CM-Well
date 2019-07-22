<template>
  <div class="main-container">
    <!-- <cmwell-header></cmwell-header> -->
    <div class="flexy-container">
      <infoton-list />
      <infoton />
    </div>
    <!-- <cmwell-footer></cmwell-footer> -->
  </div>
</template>

<script>
  // @ is an alias to /src
  import Footer from '@/components/Footer.vue'
  import Header from '@/components/Header.vue'
  import Infoton from '@/components/Infoton/Infoton.vue'
  import InfotonList from '@/components/Infoton/InfotonList.vue'

  export default {
    name: 'home',
    components: {
      cmwellHeader: Header,
      Infoton,
      InfotonList,
      cmwellFooter: Footer
    }
  }
</script>

<style>
</style>
