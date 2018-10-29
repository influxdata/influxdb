// Libraries
import _ from 'lodash'

// APIs
import {analyzeQueries} from 'src/shared/apis'

// Utils
import replaceTemplates, {replaceInterval} from 'src/tempVars/utils/replace'
import {getDeep} from 'src/utils/wrappers'
import AJAX from 'src/utils/ajax'
import {parseResponse} from 'src/shared/parsing/flux/response'

// Types
import {Template, FluxTable} from 'src/types'
import {DashboardQuery} from 'src/types/v2/dashboards'

export const fetchTimeSeries = async (
  link: string,
  queries: DashboardQuery[],
  resolution: number,
  templates: Template[]
): Promise<FluxTable[]> => {
  const timeSeriesPromises = queries.map(async ({type, text}) => {
    try {
      const query = await replace(text, link, templates, resolution)
      const dialect = {
        header: true,
        annotations: ['datatype', 'group', 'default'],
        delimiter: ',',
      }

      const {data} = await AJAX({
        method: 'POST',
        url: link,
        data: {
          type,
          query,
          dialect,
        },
      })

      return data
    } catch (error) {
      console.error(error)
      throw error
    }
  })

  try {
    const responses = await Promise.all(timeSeriesPromises)
    const tables = _.flatten(responses.map(r => parseResponse(r)))
    return tables
  } catch (error) {
    console.error(error)
    throw error
  }
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
