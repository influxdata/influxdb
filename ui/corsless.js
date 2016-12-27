const express = require('express');
const request = require('request');

const app = express();

app.use('/', (req, res) => {
  console.log(`${req.method} ${req.url}`);

  const headers = {};
  headers['Access-Control-Allow-Origin'] = '*';
  headers['Access-Control-Allow-Methods'] = 'POST, GET, PUT, DELETE, OPTIONS';
  headers['Access-Control-Allow-Credentials'] = false;
  headers['Access-Control-Max-Age'] = '86400'; // 24 hours
  headers['Access-Control-Allow-Headers'] = 'X-Requested-With, X-HTTP-Method-Override, Content-Type, Accept';
  res.writeHead(200, headers);

  if (req.method === 'OPTIONS') {
    res.end();
  }
  else {
    const url = 'http://localhost:8888' + req.url;
    req.pipe(request(url)).pipe(res);
  }
});

app.listen(3888, () => {
  console.log('corsless proxy server now running')
});
