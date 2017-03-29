export function fetchTimeSeries(host, query) {
  return {
    type: 'REQUEST_TIME_SERIES',
    meta: {
      query: true,
    },
    payload: {
      host,
      query,
    },
  }
}
