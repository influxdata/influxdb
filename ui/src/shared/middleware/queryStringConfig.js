// Middleware generally used for actions needing parsed queryStrings
import queryString from 'query-string'

import {enablePresentationMode} from 'src/shared/actions/app'
import {setDashTimeV1} from 'src/dashboards/actions'
import {notify as notifyAction} from 'shared/actions/notifications'
import {notifyInvalidTimeRangeValueInURLQuery} from 'shared/copy/notifications'
import {timeRanges, defaultTimeRange} from 'src/shared/data/timeRanges'
import idNormalizer, {TYPE_ID} from 'src/normalizers/id'
import {validTimeRange} from 'src/dashboards/utils/time'

export const queryStringConfig = store => {
  let prevPath
  return dispatch => action => {
    dispatch(action)
    const urlQueries = queryString.parse(window.location.search)

    // Presentation Mode
    if (urlQueries.present === 'true') {
      dispatch(enablePresentationMode())
    }

    const dashboardRegex = /\/sources\/\d+\/dashboards\/(\d+)/
    if (dashboardRegex.test(window.location.pathname)) {
      const currentPath = window.location.pathname
      const dashboardID = currentPath.match(dashboardRegex)[1]
      if (currentPath !== prevPath) {
        const {dashTimeV1} = store.getState()

        const dashboardTimeRange = dashTimeV1.ranges.find(
          r => r.dashboardID === idNormalizer(TYPE_ID, dashboardID)
        )

        let timeRange = dashboardTimeRange

        const isValidTimeRange = validTimeRange(timeRange)
        if (isValidTimeRange) {
          if (urlQueries.upper) {
            timeRange = {
              ...timeRange,
              upper: urlQueries.upper,
              lower: urlQueries.lower,
            }
          } else {
            timeRange = timeRanges.find(t => t.lower === urlQueries.lower)
          }
        } else {
          dispatch(
            notifyAction(notifyInvalidTimeRangeValueInURLQuery(timeRange))
          )
          timeRange = defaultTimeRange
        }

        dispatch(setDashTimeV1(+dashboardID, timeRange))
      }
      prevPath = currentPath
    }
  }
}
