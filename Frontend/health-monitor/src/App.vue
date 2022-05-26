<template>
  <v-app >
  <Header></Header>
    <v-main class="brown lighten-3 ">
      <v-btn class="my-9 mx-7" color ="#1C0A00" style="font-weight:bold" dark rounded @click="GetStatistics()">Get statistics </v-btn>
      <div>
        <v-row>
          <h2 class="ml-10">From Date</h2>
          <v-spacer></v-spacer>
          <h2 class="mr-10">To Date</h2>
        </v-row>
        <v-row class="mb-4">
          <v-datetime-picker dark class="ml-10"  color ="#1C0A00" label="Date-Time" v-model="from"> </v-datetime-picker>
          <v-spacer></v-spacer>
          <v-datetime-picker dark class="ml-10"  color ="#1C0A00" label="Date-Time" v-model="to"> </v-datetime-picker>
        </v-row>
      </div>
      <div  class="ml-7 my-2" v-if="show">
        <h1>Evaluation</h1>
      </div>
      <div class="ml-7 mt-1" v-for="stat in evaluation" :key="stat" >
        <p> {{stat}} </p>
      </div>
      
    </v-main>
  </v-app>
</template>

<script>
import axios from 'axios'
import Header from './components/Header.vue';
import Vue from 'vue'
import DatetimePicker from 'vuetify-datetime-picker'
Vue.use(DatetimePicker)
export default {
  name: 'App',

  components: {
    Header
  },

  data() {
    return {
      from : "",
      to : "",
      statistics : [],
      show : false,
      evaluation:[],
    };
  },
  methods:{
    GetStatistics(){
      axios.get('http://localhost:8080/api/getstatistics',{
        params : {
          startDate : new Date(this.from),
          endDate : new Date(this.to)
        }
      })
        .then((response) => {
            console.log(response.data)
            this.statistics = response.data
            let j=0
            for (let i = 0; i < this.statistics.length; i ++) {
                this.evaluation[i] = this.statistics[j];
                j+=1
                if(j == this.statistics.length)
                    break;
            }
            this.show = true
        })
        .catch((error) => {
          console.log(error)
        })
    }
  }
};
</script>
