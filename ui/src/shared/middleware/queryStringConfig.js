// Middleware generally used for actions needing parsed queryStrings
import queryString from 'query-string'

import {enablePresentationMode} from 'src/shared/actions/app'
import {templateVariablesSelectedByName} from 'src/dashboards/actions'

export const queryStringConfig = () => next => action => {
  next(action)
  const qs = queryString.parse(window.location.search)

  // Presentation Mode
  if (qs.present === 'true') {
    next(enablePresentationMode())
  }

  // Select Template Variable By Name
  const dashboardRegex = /\/sources\/(\d+?)\/dashboards\/(\d+?)/
  if (dashboardRegex.test(window.location.pathname)) {
    const dashboardID = window.location.pathname.match(dashboardRegex)[2]
    next(templateVariablesSelectedByName(+dashboardID, qs))
  }
}
