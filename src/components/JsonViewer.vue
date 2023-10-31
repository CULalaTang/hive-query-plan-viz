<template>
  <div class="my-container" style="padding-bottom: 8px">
    <span class="demonstration">JSON格式化</span>
    <vue-json-pretty :data="jsonObj" :deep="2"></vue-json-pretty>
  </div>
</template>

<script>
import VueJsonPretty from 'vue-json-pretty';
import 'vue-json-pretty/lib/styles.css';
import eventBus from "../eventBus/event-bus";

export default {
  name: "JsonViewer",
  components: {
    VueJsonPretty,
  },
  data: function () {
    return {
      jsonObj: {}
    }
  },
  created() {
    eventBus.$on('textChange', (text) => {
      this.jsonObj = text
    })
  }
}
</script>
<style scoped>

</style>