import {proxy} from 'utils/queryUrlGenerator'
import {editQueryStatus} from 'src/data_explorer/actions/view'
import _ from 'lodash'

export const handleLoading = (query, dispatch) => {
  dispatch(editQueryStatus(query.id, {loading: true}))
}
// {results: [{}]}
export const handleSuccess = (data, query, dispatch) => {
  const {results} = data
  const error = _.get(results, ['0', 'error'], false)
  const series = _.get(results, ['0', 'series'], false)
  // 200 from server and no results = warn
  if (!series && !error) {
    dispatch(editQueryStatus(query.id, {warn: 'Your query is syntactically correct but returned no results'}))
    return data
  }

  // 200 from chrono server but influx returns an "error" = warning
  if (error) {
    dispatch(editQueryStatus(query.id, {warn: error}))
    return data
  }

  // 200 from server and results contains data = success
  dispatch(editQueryStatus(query.id, {success: 'Success!'}))
  return data
}

export const handleError = (error, query, dispatch) => {
  const message = _.get(error, ['data', 'message'], error)

  // 400 from chrono server = fail
  dispatch(editQueryStatus(query.id, {error: message}))
  console.error(error)
}

export const fetchTimeSeriesAsync = ({source, db, rp, query}) => async (dispatch) => {
  handleLoading(query, dispatch)
  try {
    const {data} = await proxy({source, db, rp, query: query.text})
    return handleSuccess(data, query, dispatch)
  } catch (error) {
    handleError(error, query, dispatch)
    throw error
  }
}
