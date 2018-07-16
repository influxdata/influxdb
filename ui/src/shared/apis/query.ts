import _ from 'lodash'
import {getDeep} from 'src/utils/wrappers'
import {
  handleSuccess,
  handleError,
  handleLoading,
} from 'src/shared/actions/timeSeries'
import {analyzeQueries} from 'src/shared/apis'
import {DEFAULT_DURATION_MS} from 'src/shared/constants'
import replaceTemplates, {replaceInterval} from 'src/tempVars/utils/replace'
import {proxy} from 'src/utils/queryUrlGenerator'

import {Source} from 'src/types'

import {Template} from 'src/types'

const noop = () => ({
  type: 'NOOP',
  payload: {},
})
interface Query {
  text: string
  database?: string
  db?: string
  rp?: string
  id: string
}

export const fetchTimeSeries = async (
  source: Source,
  queries: Query[],
  resolution: number,
  templates: Template[],
  editQueryStatus: () => any = noop
) => {
  const timeSeriesPromises = queries.map(async query => {
    try {
      const text = await replace(query.text, source, templates, resolution)
      return handleQueryFetchStatus({...query, text}, source, editQueryStatus)
    } catch (error) {
      console.error(error)
      throw error
    }
  })

  return Promise.all(timeSeriesPromises)
}

const handleQueryFetchStatus = async (
  query: Query,
  source: Source,
  editQueryStatus: () => any
) => {
  const {database, rp} = query
  const db = _.get(query, 'db', database)

  try {
    handleLoading(query, editQueryStatus)

    const payload = {
      source: source.links.proxy,
      db,
      rp,
      query: query.text,
    }

    const {data} = await proxy(payload)

    return handleSuccess(data, query, editQueryStatus)
  } catch (error) {
    console.error(error)
    handleError(error, query, editQueryStatus)
    throw error
  }
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

export const duration = async (
  query: string,
  source: Source
): Promise<number> => {
  try {
    const analysis = await analyzeQueries(source.links.queries, [{query}])
    return getDeep<number>(analysis, '0.durationMs', DEFAULT_DURATION_MS)
  } catch (error) {
    console.error(error)
    throw error
  }
}
