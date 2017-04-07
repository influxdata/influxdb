import {proxy} from 'utils/queryUrlGenerator'
import {editQueryStatus} from 'src/data_explorer/actions/view'
import _ from 'lodash'

export const fetchTimeSeriesAsync = (source, db, query) => async (dispatch) => {
  dispatch(editQueryStatus(query.id, {loading: true}))

  try {
    const {data} = await proxy({source, db, query: query.text})
    const results = _.get(data, ['results', '0'], false)
    const warn = _.get(results, 'error', false)

    // 200 from server and no results = warn
    if (_.isEmpty(results)) {
      dispatch(editQueryStatus(query.id, {warn: 'Your query is syntactically correct but returned no results'}))
      return results
    }

    // 200 from chrono server but influx returns an error = warning
    if (warn) {
      dispatch(editQueryStatus(query.id, {warn}))
      return warn
    }

    // 200 from server and results contains data = success
    dispatch(editQueryStatus(query.id, {success: 'Success!'}))
    return results
  } catch (error) {
    const message = _.get(error, ['data', 'message'], error)

    // 400 from chrono server = fail
    dispatch(editQueryStatus(query.id, {error: message}))
    console.error(error)
    throw error
  }
}
