import _ from 'lodash'
import {getDeep} from 'src/utils/wrappers'
import {fetchTimeSeriesAsync} from 'src/shared/actions/timeSeries'
import {analyzeQueries} from 'src/shared/apis'
import replaceTemplates, {replaceInterval} from 'src/tempVars/utils/replace'
import {Source} from 'src/types'

import {Template} from 'src/types'

interface Query {
  text: string
  database: string
  db: string
  rp: string
  id: string
}

export const fetchTimeSeries = async (
  source: Source,
  queries: Query[],
  resolution: number,
  templates: Template[],
  editQueryStatus: () => any
) => {
  const timeSeriesPromises = queries.map(async query => {
    const {database, rp} = query
    const db = _.get(query, 'db', database)

    try {
      const text = await replace(query.text, source, templates, resolution)

      const payload = {
        source: source.links.proxy,
        db,
        rp,
        query: {...query, text},
      }

      return fetchTimeSeriesAsync(payload, editQueryStatus)
    } catch (error) {
      console.error(error)
      throw error
    }
  })

  return Promise.all(timeSeriesPromises)
}

const replace = async (
  query: string,
  source: Source,
  templates: Template[],
  resolution: number
): Promise<string> => {
  try {
    query = replaceTemplates(query, templates)
    const durationMs = await duration(query, source)
    return replaceInterval(query, Math.floor(resolution / 3), durationMs)
  } catch (error) {
    console.error(error)
    throw error
  }
}

const duration = async (query: string, source: Source): Promise<number> => {
  try {
    const analysis = await analyzeQueries(source.links.queries, [{query}])
    return getDeep<number>(analysis, '0.durationMs', 1000)
  } catch (error) {
    console.error(error)
    throw error
  }
}
