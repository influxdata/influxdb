import {buildQuery} from 'utils/influxql'
import {TYPE_QUERY_CONFIG, TYPE_SHIFTED} from 'src/dashboards/constants'

const buildQueries = (proxy, queryConfigs, tR) => {
  const statements = queryConfigs.map(query => {
    const {rawText, range, id, shifts, database, measurement, fields} = query
    const timeRange = range || tR
    const text = rawText || buildQuery(TYPE_QUERY_CONFIG, timeRange, query)
    const isParsable = database && measurement && fields.length

    if (shifts && shifts.length && isParsable) {
      const shiftedQueries = shifts
        .filter(s => s.unit)
        .map(s => buildQuery(TYPE_SHIFTED, timeRange, query, s))

      return {
        text: `${text};${shiftedQueries.join(';')}`,
        id,
        queryConfig: query,
      }
    }

    return {text, id, queryConfig: query}
  })

  const queries = statements
    .filter(s => s.text !== null)
    .map(({queryConfig, text, id}) => {
      let queryProxy = ''
      if (queryConfig.source) {
        queryProxy = `${queryConfig.source.links.proxy}`
      }

      const host = [queryProxy || proxy]

      return {
        host,
        text,
        id,
        queryConfig,
      }
    })

  return queries
}

export default buildQueries
