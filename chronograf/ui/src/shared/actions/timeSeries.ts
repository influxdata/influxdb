import {TimeSeriesResponse, TimeSeriesSeries} from 'src/types/series'
import {Status} from 'src/types'

import {getDeep} from 'src/utils/wrappers'

interface Query {
  text: string
  id: string
  database?: string
  db?: string
  rp?: string
}

type EditQueryStatusFunction = (queryID: string, status: Status) => void

export const handleLoading = (
  query: Query,
  editQueryStatus: EditQueryStatusFunction
): void =>
  editQueryStatus(query.id, {
    loading: true,
  })

export const handleSuccess = (
  data: TimeSeriesResponse,
  query: Query,
  editQueryStatus: EditQueryStatusFunction
): TimeSeriesResponse => {
  const {results} = data
  const error = getDeep<string>(results, '0.error', null)
  const series = getDeep<TimeSeriesSeries>(results, '0.series', null)
  // 200 from server and no results = warn
  if (!series && !error) {
    editQueryStatus(query.id, {
      warn: 'Your query is syntactically correct but returned no results',
    })
    return data
  }

  // 200 from chrono server but influx returns an "error" = warning
  if (error) {
    editQueryStatus(query.id, {
      warn: error,
    })
    return data
  }

  // 200 from server and results contains data = success
  editQueryStatus(query.id, {
    success: 'Success!',
  })
  return data
}

export const handleError = (
  error,
  query: Query,
  editQueryStatus: EditQueryStatusFunction
): void => {
  const message =
    getDeep<string>(error, 'data.message', '') ||
    getDeep<string>(error, 'message', 'Could not retrieve data')

  // 400 from chrono server = fail
  editQueryStatus(query.id, {
    error: message,
  })
}
