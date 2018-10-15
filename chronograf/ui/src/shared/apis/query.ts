import {getDeep} from 'src/utils/wrappers'
import {analyzeQueries} from 'src/shared/apis'
import {DEFAULT_DURATION_MS} from 'src/shared/constants'
import replaceTemplates, {replaceInterval} from 'src/tempVars/utils/replace'
import {proxy} from 'src/utils/queryUrlGenerator'

import {Template} from 'src/types'

interface Query {
  id: string
  text: string
}

export const fetchTimeSeries = async (
  link: string,
  queries: Query[],
  resolution: number,
  templates: Template[]
) => {
  const timeSeriesPromises = queries.map(async ({text}) => {
    try {
      const query = await replace(text, link, templates, resolution)
      const payload = {source: link, query}
      const {data} = await proxy(payload)
      return data
    } catch (error) {
      console.error(error)
      throw error
    }
  })

  return Promise.all(timeSeriesPromises)
}

const replace = async (
  query: string,
  __: string, // this field is the link to analyze the query duration
  templates: Template[],
  resolution: number
): Promise<string> => {
  try {
    query = replaceTemplates(query, templates)
    // TODO: analyze query duration for interval
    // const durationMs = await duration(query, link)
    return replaceInterval(query, Math.floor(resolution / 3), 1000 * 60 * 60)
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
