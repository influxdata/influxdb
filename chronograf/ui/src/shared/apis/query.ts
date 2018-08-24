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

import {Template} from 'src/types'

const noop = () => ({
  type: 'NOOP',
  payload: {},
})

interface Query {
  id: string
  text: string
}

export const fetchTimeSeries = async (
  link: string,
  queries: Query[],
  resolution: number,
  templates: Template[],
  editQueryStatus: () => any = noop
) => {
  const timeSeriesPromises = queries.map(async query => {
    try {
      const text = await replace(query.text, link, templates, resolution)
      return handleQueryFetchStatus({...query, text}, link, editQueryStatus)
    } catch (error) {
      console.error(error)
      throw error
    }
  })

  return Promise.all(timeSeriesPromises)
}

const handleQueryFetchStatus = async (
  query: Query,
  link: string,
  editQueryStatus: () => any
) => {
  try {
    handleLoading(query, editQueryStatus)

    const payload = {
      source: link,
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
  link: string,
  templates: Template[],
  resolution: number
): Promise<string> => {
  try {
    query = replaceTemplates(query, templates)
    const durationMs = await duration(query, link)
    return replaceInterval(query, Math.floor(resolution / 3), durationMs)
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const duration = async (
  query: string,
  link: string
): Promise<number> => {
  try {
    const analysis = await analyzeQueries(link, [{query}])
    return getDeep<number>(analysis, '0.durationMs', DEFAULT_DURATION_MS)
  } catch (error) {
    console.error(error)
    throw error
  }
}
