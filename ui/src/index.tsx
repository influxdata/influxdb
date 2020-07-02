import 'babel-polyfill'
import 'abortcontroller-polyfill/dist/polyfill-patch-fetch'

import React, {PureComponent} from 'react'
import {render} from 'react-dom'
import {Provider} from 'react-redux'
import {Route} from 'react-router-dom'
import {ConnectedRouter} from 'connected-react-router'

// import {CLOUD} from 'src/shared/constants'
import configureStore, {history} from 'src/store/configureStore'
import {loadLocalStorage} from 'src/localStorage'

import {getRootNode} from 'src/utils/nodes'
import {getBrowserBasepath} from 'src/utils/basepath'
import {updateReportingContext} from 'src/cloud/utils/reporting'

// Components
import Setup from 'src/Setup'
import NotFound from 'src/shared/components/NotFound'
import GetLinks from 'src/shared/containers/GetLinks'

// Utilities
import {writeNavigationTimingMetrics} from 'src/cloud/utils/rum'

// Actions
import {disablePresentationMode} from 'src/shared/actions/app'

// Styles
import 'src/style/chronograf.scss'
import '@influxdata/clockface/dist/index.css'

const rootNode = getRootNode()
const basepath = getBrowserBasepath()

const SESSION_KEY = 'session'

const cookieSession = document.cookie.match(
  new RegExp('(^| )' + SESSION_KEY + '=([^;]+)')
)

updateReportingContext({
  session: cookieSession ? cookieSession[2].slice(5) : '',
})

declare global {
  interface Window {
    basepath: string
    dataLayer: any[]
  }
}

// Older method used for pre-IE 11 compatibility
window.basepath = basepath

export const store = configureStore(loadLocalStorage())
const {dispatch} = store

if (window['Cypress']) {
  window['store'] = store
}

history.listen(() => {
  dispatch(disablePresentationMode())
})

window.addEventListener('keyup', event => {
  const escapeKeyCode = 27
  // fallback for browsers that don't support event.key
  if (event.key === 'Escape' || event.keyCode === escapeKeyCode) {
    dispatch(disablePresentationMode())
  }
})

class Root extends PureComponent {
  public render() {
    return (
      <Provider store={store}>
        <ConnectedRouter history={history}>
          <Route component={GetLinks} />
          <Route component={Setup} />
          <Route component={NotFound} />
        </ConnectedRouter>
      </Provider>
    )
  }
}

if (rootNode) {
  render(<Root />, rootNode)
}

window.addEventListener('load', () => {
  writeNavigationTimingMetrics()
})
