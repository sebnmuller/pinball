const axios = require('axios');
const constants = require('./constants.js');
const elasticsearch = require('elasticsearch');

// Get data from API
axios.get(constants.base_url + '&start_pos=1&count=1')
  .then(initial_response => {

    var total_count = initial_response.data.total_count;

    console.log(total_count);

    var max_loops = Math.ceil(initial_response.data.total_count / 500);
    var max_start_pos = 25000;

    players = retrieve_data(max_start_pos, 500);

    elasticsearch_stuff(players);
    //elasticsearch_stuff(others);

  })
  .catch(error => {
    console.log(error);
  });

  function elasticsearch_stuff(players) {

    console.log(players.length);
      var client = new elasticsearch.Client({
        host: 'localhost:9200',
        log: 'warning'
      });

      // Send data to Elasticsearch
      bulk_body = build_bulk_body(players);
      client.bulk({
        body: bulk_body
      }, function (error, response) {
        console.log(error);
      });
  }

  function build_bulk_body(players){
    bulk_body = []

    players.forEach(function (player) {
      bulk_body.push(
        { index: { _index: 'pinball', _type: 'pinball', _id: player.player_id } },
        { player }
      )
    });

    return bulk_body;
  }

  function retrieve_data(max_start_pos, count) {

    players = [];

    for (var i = 1; i*count <= max_start_pos; i++) {

      axios.get(constants.base_url + '&start_pos=' + i + '&count=' + count)
        .then(data_response => {
          data_response.data.rankings.forEach(function (player) {
            players.push(player);
          })
          console.log(players.length)
        })
        .catch(error => {
          console.log(error);
        });
    }

    elasticsearch_stuff(players)

    return players;
  }
