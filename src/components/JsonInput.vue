<template>
  <div class="my-container" style="padding: 30px">
    <el-input type="textarea" :rows="10" placeholder="请输入explain formatted 【hive sql】，结尾免“；”" v-model="textareaSql">

    </el-input>
    <el-button :disabled="isLoading" elevation="10" text @click="sendSqlStr()" style="background-color: #409eff; color: #dfe4ed;  text-align: right !important; float: right; margin-right: auto;">sendSql</el-button>

    <el-input type="textarea" :rows="10" placeholder="请输入hive query explain" v-model="textarea" style="margin-top: 50px">
    </el-input>
    <el-button elevation="10" text @click="sendJsonStr()" style="background-color: #409eff; color: #dfe4ed; text-align: right !important; float: right; margin-right: auto;">sendExplain</el-button>
    <h5 style="text-align: left; font-family: 'Arial', sans-serif; font-size: 8px; font-weight: bold; color: #333;">
        <a href="https://www.wolai.com/hNrvPjnVzBcHPvWdUXRLah" target="目标窗口的弹出方式" title=" hive sql常用优化参考">hive sql常用优化参考</a>
    </h5>

  </div>
</template>


<script>
import eventBus from "../eventBus/event-bus";
import axios from 'axios';
// axios.defaults.baseURL = 'http://localhost:8080';
// eslint-disable-next-line no-undef
// axios.defaults.headers.common['Authorization'] = AUTH_TOKEN;
// axios.defaults.headers.post['Content-Type'] = 'application/json';
export default {
  name: "JsonInput",
  data() {
    return {
      textarea: '',
      textareaSql: '',
      isLoading: false
    }
  },
  methods: {
    sendSqlStr: function () {
      this.isLoading = true;
      axios({
        method: 'post',
        url: 'http://10.80.1.153:8080/apigateway/DataOps/api/v1/getHiveExplain',
        timeout: 120000,
        data: {
          querySql: this.textareaSql
        }
      }).then(response => {
        if (response.data.code === 1){
          alert("服务器错误："+ response.data.msg)
        }else{
          let reg = /\\/g;
          let explain=JSON.stringify(response.data.data).replace(reg,'');
          this.textarea = explain.substring(1,explain.length-1);
        }
        this.isLoading = false;
      }).catch(error => {
        alert("请求异常："+error);
        this.isLoading = false;
        console.log(error)
      });
    },
    sendJsonStr: function () {
      eventBus.$emit('textChange', JSON.parse(this.textarea));
    }
  }
}
</script>

<style scoped>

</style>