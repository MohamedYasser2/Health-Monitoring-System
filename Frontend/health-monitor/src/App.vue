<template>
  <v-app>
    <v-app-bar
      app
      color="primary"
      dark
    >
    Big Data
    </v-app-bar>
    <v-main>
      <v-btn rounded @click="GetStatistics()">Get statistics </v-btn>
      <div v-for="stat in statistics" :key="stat" >
        <h3> {{stat}} </h3>
      </div>
      <div>
        <v-row>
        <v-date-picker v-model="from"></v-date-picker>
        <v-date-picker v-model="to"></v-date-picker>
        </v-row>
      </div>
    </v-main>
  </v-app>
</template>

<script>
import axios from 'axios'

export default {
  name: 'App',

  components: {
      
  },

  data() {
    return {
      from : "",
      to : "",
      statistics : [],
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
        })
        .catch((error) => {
          console.log(error)
        })
    }
  }
};
</script>
