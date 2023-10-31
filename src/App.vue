<template>
  <div id="app">
    <QueryPlanViz></QueryPlanViz>
  </div>
</template>

<script>
import QueryPlanViz from "./components/QueryPlanViz";

export default {
  name: 'app',
  components: {
    QueryPlanViz
  }
}
</script>

<style>
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
}

.my-container {
  border-radius: 4px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, .12), 0 0 6px rgba(0, 0, 0, .04);
  margin: 10px;
  padding: 5px;
}

</style>
