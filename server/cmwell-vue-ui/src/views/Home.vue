<template>
  <div class="home">
    <!-- <cmwell-header></cmwell-header> -->
    <homePage />
    <!-- <cmwell-footer></cmwell-footer> -->
  </div>
</template>

<script>
// @ is an alias to /src
import Footer from "@/components/Footer.vue";
import Header from "@/components/Header.vue";
import HomePage from "@/views/HomePage.vue";

export default {
  name: 'home',
  components: {
    cmwellHeader: Header,
    HomePage,
    cmwellFooter: Footer
  }
}
</script>
