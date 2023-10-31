<template>
  <div ref="parent">
    <el-row :gutter="0" class="my-container">
      <el-col :span="10">
        <div class="grid-content">

          <span class="demonstration">画布高度</span>
          <el-slider v-model="containerSize.height" :max="10800" :min="0"></el-slider>
        </div>
      </el-col>
      <el-col :span="10">
        <div class="grid-content">
          <span class="demonstration">画布宽度</span>
          <el-slider v-model="containerSize.width" :max="4096" :min="0"></el-slider>
        </div>
      </el-col>
      <el-col :span="4">
        <div class="grid-content">
          <el-tooltip class="item" effect="dark" content="导出图片" placement="top">
            <el-button type="primary" @click="exportGraph" icon="el-icon-download"></el-button>
          </el-tooltip>
        </div>
      </el-col>
    </el-row>
    <div id="graphContainer" ref="con" v-bind:style="containerStyle"></div>
  </div>
</template>

<script>
import eventBus from "../eventBus/event-bus";
import getGraphData from "../api/query-plan-handler"
import graphRender from "../api/graph-render"
import initG6Graph from "../api/init-g6-graph";

export default {
  name: "PlanGraph",
  data: function () {
    return {
      graphData: {},
      graph: {},
      containerSize: {
        height: 0,
        width: 0,
      }
    }
  },
  created() {
    eventBus.$on('textChange', (text) => {
      this.graphData = getGraphData(text);
      graphRender(this.graph, this.graphData);
    })
  },
  mounted() {
    this.graph = initG6Graph(this.$refs.con);
    this.containerSize.width = this.$refs.parent.offsetWidth;
    this.containerSize.height = 1000;
    console.log(this.containerSize)
  },
  methods: {
    exportGraph: function () {
      this.graph.downloadFullImage();
    }
  },
  computed: {
    containerStyle: function () {
      return {height: `${this.containerSize.height}px`, width: `${this.containerSize.width}px`};
    }
  },
  watch: {
    containerStyle: function () {
      this.graph.changeSize(this.containerSize.width, this.containerSize.height);
      this.graph.fitView();
    }
  }
}
</script>

<style scoped>
#graphContainer {
  position: relative;
}

/deep/ .g6-tooltip {
  color: #444;
  background-color: rgba(255, 255, 255, 0.9);
  border-radius: 4px;
  box-shadow: rgb(174, 174, 174) 0px 0px 10px;
}

.el-col {
  padding-left: 10px;
  padding-right: 10px;
}

.el-button {
  margin-top: 10px;
}
</style>