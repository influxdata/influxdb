import _ from 'lodash'
import {buildQuery} from 'src/utils/influxql'
import {TYPE_QUERY_CONFIG, TYPE_SHIFTED} from 'src/dashboards/constants'

import {QueryConfig, TimeRange} from 'src/types'

interface Statement {
  queryConfig: QueryConfig
  id: string
  text: string
}

interface Query {
  host: string[]
  text: string
  id: string
  queryConfig: QueryConfig
}

const buildQueries = (
  proxy: string,
  queryConfigs: QueryConfig[],
  tR: TimeRange
): Query[] => {
  const statements: Statement[] = queryConfigs.map(query => {
    const {rawText, range, id, shifts, database, measurement, fields} = query
    const timeRange: TimeRange = range || tR
    const text: string =
      rawText || buildQuery(TYPE_QUERY_CONFIG, timeRange, query)
    const isParsable: boolean =
      !_.isEmpty(database) && !_.isEmpty(measurement) && fields.length > 0

    if (shifts && shifts.length && isParsable) {
      const shiftedQueries: string[] = shifts
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

  const queries: Query[] = statements
    .filter(s => s.text !== null)
    .map(({queryConfig, text, id}) => {
      const queryProxy: string = _.get(queryConfig, 'source.links.proxy', '')

      const host: string[] = [queryProxy || proxy]

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
