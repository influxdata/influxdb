import buildInfluxQLQuery from 'utils/influxql'

const buildQueries = (proxy, queryConfigs, timeRange) => {
  const statements = queryConfigs.map(query => {
    const text =
      query.rawText || buildInfluxQLQuery(query.range || timeRange, query)
    return {text, id: query.id, queryConfig: query}
  })

  const queries = statements.filter(s => s.text !== null).map(s => {
    let queryProxy = ''
    if (s.queryConfig.source) {
      queryProxy = `${s.queryConfig.source}/proxy`
    }

    return {
      host: [queryProxy || proxy],
      text: s.text,
      id: s.id,
      queryConfig: s.queryConfig,
    }
  })

  return queries
}

export default buildQueries
