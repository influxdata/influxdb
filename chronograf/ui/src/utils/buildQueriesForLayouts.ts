import _ from 'lodash'

import {buildQuery} from 'src/utils/influxql'
import {TYPE_SHIFTED, TYPE_QUERY_CONFIG} from 'src/dashboards/constants'
import {
  TEMP_VAR_DASHBOARD_TIME,
  TEMP_VAR_UPPER_DASHBOARD_TIME,
} from 'src/shared/constants'

import {CellQuery, TimeRange} from 'src/types'

const addTimeBoundsToRawText = (rawText: string): string => {
  if (!rawText) {
    return
  }

  const dashboardTimeRegex = new RegExp(
    `time( )?>( )?${TEMP_VAR_DASHBOARD_TIME}`,
    'g'
  )
  const dashboardTimeText: string = `time > ${TEMP_VAR_DASHBOARD_TIME}`
  const isUsingTimeSelectorBounds: boolean = !_.isEmpty(
    rawText.match(dashboardTimeRegex)
  )

  if (isUsingTimeSelectorBounds) {
    const upperTimeBoundRegex = new RegExp('time( )?<', 'g')
    const hasUpperTimeBound = !_.isEmpty(rawText.match(upperTimeBoundRegex))
    if (
      rawText.indexOf(TEMP_VAR_UPPER_DASHBOARD_TIME) === -1 &&
      !hasUpperTimeBound
    ) {
      const upperDashboardTimeText = `time < ${TEMP_VAR_UPPER_DASHBOARD_TIME}`
      const fullTimeText = `${dashboardTimeText} AND ${upperDashboardTimeText}`
      const boundedQueryText = rawText.replace(dashboardTimeRegex, fullTimeText)
      return boundedQueryText
    }
  }
  return rawText
}

export const buildQueries = (
  queries: CellQuery[],
  timeRange: TimeRange
): CellQuery[] => {
  return queries.map(query => {
    let queryText: string
    // Canned dashboards use an different a schema different from queryConfig.
    if (query.queryConfig) {
      const {
        queryConfig: {database, measurement, fields, shifts, rawText, range},
      } = query
      const tR: TimeRange = range || {
        upper: TEMP_VAR_UPPER_DASHBOARD_TIME,
        lower: TEMP_VAR_DASHBOARD_TIME,
      }

      queryText =
        addTimeBoundsToRawText(rawText) ||
        buildQuery(TYPE_QUERY_CONFIG, tR, query.queryConfig)
      const isParsable: boolean =
        !_.isEmpty(database) && !_.isEmpty(measurement) && fields.length > 0

      if (shifts && shifts.length && isParsable) {
        const shiftedQueries: string[] = shifts
          .filter(s => s.unit)
          .map(s => buildQuery(TYPE_SHIFTED, timeRange, query.queryConfig, s))

        queryText = `${queryText};${shiftedQueries.join(';')}`
      }
    }

    return {...query, text: queryText}
  })
}
