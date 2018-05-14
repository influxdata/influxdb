import _ from 'lodash'
import {fetchTimeSeriesAsync} from 'src/shared/actions/timeSeries'
import {removeUnselectedTemplateValues} from 'src/dashboards/constants'

import {intervalValuesPoints} from 'src/shared/constants'

interface TemplateQuery {
  db: string
  rp: string
  influxql: string
}

interface TemplateValue {
  type: string
  value: string
  selected: boolean
}

interface Template {
  type: string
  tempVar: string
  query: TemplateQuery
  values: TemplateValue[]
}

interface Query {
  host: string | string[]
  text: string
  database: string
  db: string
  rp: string
  id: string
}

const parseSource = source => {
  if (Array.isArray(source)) {
    return _.get(source, '0', '')
  }

  return source
}

export const fetchTimeSeries = async (
  queries: Query[],
  resolution: number,
  templates: Template[],
  editQueryStatus: () => any
) => {
  const timeSeriesPromises = queries.map(query => {
    const {host, database, rp} = query
    // the key `database` was used upstream in HostPage.js, and since as of this writing
    // the codebase has not been fully converted to TypeScript, it's not clear where else
    // it may be used, but this slight modification is intended to allow for the use of
    // `database` while moving over to `db` for consistency over time
    const db = _.get(query, 'db', database)

    const templatesWithIntervalVals = templates.map(temp => {
      if (temp.tempVar === ':interval:') {
        if (resolution) {
          const values = temp.values.map(v => ({
            ...v,
            value: `${_.toInteger(Number(resolution) / 3)}`,
          }))

          return {...temp, values}
        }

        return {...temp, values: intervalValuesPoints}
      }
      return temp
    })

    const tempVars = removeUnselectedTemplateValues(templatesWithIntervalVals)

    const source = parseSource(host)
    const payload = {source, db, rp, query, tempVars, resolution}
    return fetchTimeSeriesAsync(payload, editQueryStatus)
  })

  return Promise.all(timeSeriesPromises)
}
