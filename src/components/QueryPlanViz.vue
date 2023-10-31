<template>
  <div>
    <el-container>
      <el-header>
        <h3>Hive执行计划可视化</h3>
      </el-header>
      <el-main>
        <el-row>
          <el-col :span="24">
            <div class="grid-content">
              <JsonInput></JsonInput>
            </div>
          </el-col>
        </el-row>
        <el-row>
          <el-col :span="8">
            <div class="grid-content">
              <JsonViewer></JsonViewer>
            </div>
          </el-col>
          <el-col :span="16">
            <div class="grid-content">
              <PlanGraph></PlanGraph>
            </div>
          </el-col>
        </el-row>
      </el-main>
    </el-container>
  </div>
</template>

<script>
import JsonViewer from "./JsonViewer";
import JsonInput from "./JsonInput";
import PlanGraph from "./PlanGraph";

export default {
  name: "QueryPlanViz",
  components: {PlanGraph, JsonInput, JsonViewer},
  data: function () {
    return {
      jsonObj: {}
    }
  }
}
</script>

<style scoped>
.el-header {
  background-color: #aeaeb9;
  color: #333;
}
</style>