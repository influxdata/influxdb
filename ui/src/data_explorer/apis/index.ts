import AJAX from 'src/utils/ajax'
import _ from 'lodash'
import moment from 'moment'
import download from 'src/external/download'

import {proxy} from 'src/utils/queryUrlGenerator'
import {timeSeriesToTableGraph} from 'src/utils/timeSeriesTransformers'
import {dataToCSV} from 'src/shared/parsing/dataToCSV'
import {Source, QueryConfig} from 'src/types'
import {duration} from 'src/shared/apis/query'
import {replaceInterval} from 'src/tempVars/utils/replace'

export const writeLineProtocol = async (
  source: Source,
  db: string,
  data: string
): Promise<void> =>
  await AJAX({
    url: `${source.links.write}?db=${db}`,
    method: 'POST',
    data,
  })

interface DeprecatedQuery {
  id: string
  host: string
  queryConfig: QueryConfig
  text: string
}

export const getDataForCSV = (
  source: Source,
  query: DeprecatedQuery,
  errorThrown
) => async () => {
  try {
    let queryString = query.text

    if (queryString.includes(':interval:')) {
      const queryDuration = await duration(query.text, source)

      queryString = replaceInterval(query.text, null, queryDuration)
    }

    const response = await fetchTimeSeriesForCSV({
      source: source.links.proxy,
      query: queryString,
    })

    const {data} = timeSeriesToTableGraph([{response}])
    const name = csvName(query.queryConfig)
    download(dataToCSV(data), `${name}.csv`, 'text/plain')
  } catch (error) {
    errorThrown(error, 'Unable to download .csv file')
    console.error(error)
  }
}

const fetchTimeSeriesForCSV = async ({source, query}) => {
  try {
    const {data} = await proxy({source, query})
    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}

const csvName = (query: QueryConfig): string => {
  const db = _.get(query, 'database', '')
  const rp = _.get(query, 'retentionPolicy', '')
  const measurement = _.get(query, 'measurement', '')

  const timestring = moment().format('YYYY-MM-DD-HH-mm')
  return `${db}.${rp}.${measurement}.${timestring}`
}
