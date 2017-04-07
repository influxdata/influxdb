const express = require('express')
const request = require('request')
const {default: storybook} = require('@kadira/storybook/dist/server/middleware')

const app = express()

const handler = (req, res) => {
  console.log(`${req.method} ${req.url}`)
  const url = 'http://localhost:8888' + req.url
  req.pipe(request(url)).pipe(res)
}

app.use(storybook('./.storybook'))
app.get('/chronograf/v1/*', handler)
app.post('/chronograf/v1/*', handler)

app.listen(6006, () => {
  console.log('storybook proxy server now running')
})
