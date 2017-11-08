import {buildQuery} from 'utils/influxql'
import {shiftTimeRange} from 'shared/query/helpers'
import {TYPE_QUERY_CONFIG, TYPE_SHIFTED} from 'src/dashboards/constants'

const buildQueries = (proxy, queryConfigs, tR) => {
  const statements = queryConfigs.map(query => {
    const {rawText, range, id, shift, database, measurement, fields} = query
    const timeRange = range || tR
    const text = rawText || buildQuery(TYPE_QUERY_CONFIG, timeRange, query)
    const isParsable = database && measurement && fields.length

    if (shift && isParsable) {
      const shiftedQuery = buildQuery(TYPE_SHIFTED, timeRange, query, shift)

      return {text: `${text};${shiftedQuery}`, id, queryConfig: query}
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
