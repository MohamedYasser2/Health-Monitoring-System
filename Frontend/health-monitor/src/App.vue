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
        <v-date-picker dark class="ml-10"  color ="#1C0A00" v-model="from"></v-date-picker>
        <v-spacer></v-spacer>
        <v-date-picker class="mr-10" dark color ="#1C0A00"  v-model="to"></v-date-picker>
        </v-row>
      </div>
      <div  class="ml-7 my-2" v-if="show">
        <h1>Evaluation</h1>
      </div>
      <div class="ml-7 mt-1" v-for="stat in statistics" :key="stat" >
        <h3> {{stat}} </h3>
      </div>
      
    </v-main>
  </v-app>
</template>

<script>
import axios from 'axios'
import Header from './components/Header.vue';

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
    };
  },
  methods:{
    GetStatistics(){
      axios.get('http://localhost:8080/api/getstatistics',{
        params : {
          startDate : this.from,
          endDate : this.to
        }
      })
        .then((response) => {
            console.log(response.data)
            this.statistics = response.data
            this.show = true
        })
        .catch((error) => {
          console.log(error)
        })
    }
  }
};
</script>
