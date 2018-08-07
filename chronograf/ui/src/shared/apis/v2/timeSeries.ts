// APIs
import {analyzeQueries} from 'src/shared/apis'

// Utils
import replaceTemplates, {replaceInterval} from 'src/tempVars/utils/replace'
import {getDeep} from 'src/utils/wrappers'
import AJAX from 'src/utils/ajax'

// Types
import {Template} from 'src/types'

export const fetchTimeSeries = async (
  link: string,
  queries: string[],
  resolution: number,
  templates: Template[]
) => {
  const timeSeriesPromises = queries.map(async q => {
    try {
      const query = await replace(q, link, templates, resolution)

      const {data} = await AJAX({
        method: 'POST',
        url: link,
        data: {
          type: 'influxql',
          query,
        },
      })

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
  __: string,
  templates: Template[],
  resolution: number
): Promise<string> => {
  try {
    query = replaceTemplates(query, templates)

    // TODO: get actual durationMs
    // const durationMs = await duration(query, link)
    const durationMs = 1000
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
    const defaultDurationMs = 1000
    return getDeep<number>(analysis, '0.durationMs', defaultDurationMs)
  } catch (error) {
    console.error(error)
    throw error
  }
}
