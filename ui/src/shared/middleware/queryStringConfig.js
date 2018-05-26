import _ from 'lodash'
// Middleware generally used for actions needing parsed queryStrings
import queryString from 'query-string'

import {enablePresentationMode} from 'src/shared/actions/app'
import {
  templateVariablesSelectedByName,
  updateTemplateVariableOverride,
} from 'src/dashboards/actions'

export const queryStringConfig = () => {
  let prevPath
  return next => action => {
    next(action)
    const queries = queryString.parse(window.location.search)

    // Presentation Mode
    if (queries.present === 'true') {
      next(enablePresentationMode())
    }

    // Select Template Variable By Name
    const dashboardRegex = /\/sources\/(\d+?)\/dashboards\/(\d+?)/
    if (dashboardRegex.test(window.location.pathname)) {
      const currentPath = window.location.pathname
      const dashboardID = currentPath.match(dashboardRegex)[2]
      if (currentPath !== prevPath) {
        next(templateVariablesSelectedByName(+dashboardID, queries))
        _.each(queries, (v, k) => {
          const query = {[k]: v}
          next(updateTemplateVariableOverride(+dashboardID, query))
        })
      }

      prevPath = currentPath
    }
  }
}
