// Middleware generally used for actions needing parsed queryStrings
import queryString from 'query-string'
import uuid from 'node-uuid'

import {enablePresentationMode} from 'src/shared/actions/app'
import {editRawTextAsync} from 'src/data_explorer/actions/view'
import {templateVariablesSelectedByName} from 'src/dashboards/actions'

export const queryStringConfig = store => next => action => {
  next(action)
  const qs = queryString.parse(window.location.search)

  // Presentation Mode
  if (qs.present === 'true') {
    next(enablePresentationMode())
  }

  // Data Explorer Query Config
  if (qs.query) {
    const query = decodeURIComponent(qs.query)
    const state = store.getState()
    const defaultSource = state.sources.find(source => source.default)
    let id = window.location.hash // Stored on hash to prevent page reload

    if (defaultSource && !id) {
      // Find query by raw text
      for (const qid in state.dataExplorerQueryConfigs) {
        const qc = state.dataExplorerQueryConfigs[qid]
        if (qc && qc.rawText === query) {
          id = qid
        }
      }

      id = id || uuid.v4()

      qs.queryID = id
      window.location.hash = id
    }

    if (defaultSource && !state.dataExplorerQueryConfigs[id]) {
      const url = defaultSource.links.queries
      editRawTextAsync(url, id, query)(next)
    }
  }

  // Select Template Variable By Name
  const dashboardRegex = /\/sources\/(\d+?)\/dashboards\/(\d+?)/
  if (dashboardRegex.test(window.location.pathname)) {
    const dashboardID = window.location.pathname.match(dashboardRegex)[2]
    next(templateVariablesSelectedByName(+dashboardID, qs))
  }
}
