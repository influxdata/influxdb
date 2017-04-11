import {proxy} from 'utils/queryUrlGenerator'
import _ from 'lodash'

const NOOP = () => ({
  type: 'I_NEED_TO_EDIT_QUERY_STATUS',
})

export const handleLoading = (query, editQueryStatus, dispatch) => {
  dispatch(editQueryStatus(query.id, {loading: true}))
}
// {results: [{}]}
export const handleSuccess = (data, query, editQueryStatus, dispatch) => {
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

export const handleError = (error, query, editQueryStatus, dispatch) => {
  const message = _.get(error, ['data', 'message'], error)

  // 400 from chrono server = fail
  dispatch(editQueryStatus(query.id, {error: message}))
  console.error(error)
}

export const fetchTimeSeriesAsync = ({source, db, rp, query}, editQueryStatus = NOOP) => async (dispatch) => {
  handleLoading(query, editQueryStatus, dispatch)
  try {
    const {data} = await proxy({source, db, rp, query: query.text})
    return handleSuccess(data, query, editQueryStatus, dispatch)
  } catch (error) {
    handleError(error, query, editQueryStatus, dispatch)
    throw error
  }
}
