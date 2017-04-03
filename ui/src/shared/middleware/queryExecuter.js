import AJAX from 'utils/ajax'

// makeQueryExecuter responds to actions that need to make a query to InfluxDB
export default function makeQueryExecuter() {
  return _ => next => action => { // eslint-disable-line arrow-parens
    if (action.meta && action.meta.query) {
      const {host, query} = action.payload

      _fetchTimeSeries(host, query)
      .then((timeSeries) => {
        next({
          type: 'LOAD_TIME_SERIES',
          payload: {timeSeries, host, query},
        })
      })
      .catch((err) => {
        console.error('Error occured while fetching time series: ', err.toString()) // eslint-disable-line no-console
      })
    }

    next(action)
  }
}

function _fetchTimeSeries(host, query) {
  const db = '_internal'
  const url = encodeURIComponent(`http://${host}/query?db=${db}&epoch=ms&q=${query}`)

  return AJAX({
    url: `/proxy?proxy_url=${url}`,
  })
}
