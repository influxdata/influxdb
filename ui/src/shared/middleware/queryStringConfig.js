// Middleware generally used for actions needing parsed queryStrings
import queryString from 'query-string'

import {enablePresentationMode} from 'src/shared/actions/app'
import {templateVariablesSelectedByName} from 'src/dashboards/actions'

export const queryStringConfig = () => {
  let prevPath
  return next => action => {
    next(action)
    const qs = queryString.parse(window.location.search)

    // Presentation Mode
    if (qs.present === 'true') {
      next(enablePresentationMode())
    }

    // Select Template Variable By Name
    const dashboardRegex = /\/sources\/(\d+?)\/dashboards\/(\d+?)/
    if (dashboardRegex.test(window.location.pathname)) {
      const currentPath = window.location.pathname
      const dashboardID = currentPath.match(dashboardRegex)[2]
      if (currentPath !== prevPath) {
        next(templateVariablesSelectedByName(+dashboardID, qs))
      }

      prevPath = currentPath
    }
  }
}
