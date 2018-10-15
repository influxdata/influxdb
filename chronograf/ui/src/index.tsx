import 'babel-polyfill'

import React, {PureComponent} from 'react'
import {render} from 'react-dom'
import {Provider} from 'react-redux'
import {Router, Route, useRouterHistory} from 'react-router'
import {createHistory, History} from 'history'
import {bindActionCreators} from 'redux'

import configureStore from 'src/store/configureStore'
import {loadLocalStorage} from 'src/localStorage'

import {getRootNode} from 'src/utils/nodes'
import {getBasepath} from 'src/utils/basepath'

// Components
import App from 'src/App'
import CheckSources from 'src/CheckSources'
import Setup from 'src/Setup'
import Signin from 'src/Signin'
import TaskPage from 'src/tasks/containers/TaskPage'
import TasksPage from 'src/tasks/containers/TasksPage'
import {DashboardsPage, DashboardPage} from 'src/dashboards'
import DataExplorerPage from 'src/dataExplorer/components/DataExplorerPage'
import {SourcePage, ManageSources} from 'src/sources'
import {UserPage} from 'src/user'
import {LogsPage} from 'src/logs'
import NotFound from 'src/shared/components/NotFound'

// Actions
import {getLinksAsync} from 'src/shared/actions/links'
import {disablePresentationMode} from 'src/shared/actions/app'

// Styles
import 'src/style/chronograf.scss'

const rootNode = getRootNode()
const basepath = getBasepath()

declare global {
  interface Window {
    basepath: string
  }
}

// Older method used for pre-IE 11 compatibility
window.basepath = basepath

const history: History = useRouterHistory(createHistory)({
  basename: basepath, // this is written in when available by the URL prefixer middleware
})

const store = configureStore(loadLocalStorage(), history)
const {dispatch} = store

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

interface State {
  ready: boolean
}

class Root extends PureComponent<{}, State> {
  private getLinks = bindActionCreators(getLinksAsync, dispatch)

  constructor(props) {
    super(props)
    this.state = {
      ready: false,
    }
  }

  public async componentDidMount() {
    try {
      await this.getLinks()
      this.setState({ready: true})
    } catch (error) {
      console.error('Could not get links')
    }
  }

  public render() {
    return this.state.ready ? (
      <Provider store={store}>
        <Router history={history}>
          <Route component={Setup}>
            <Route component={Signin}>
              <Route component={App}>
                <Route path="/" component={CheckSources}>
                  <Route
                    path="dashboards/:dashboardID"
                    component={DashboardPage}
                  />
                  <Route path="tasks" component={TasksPage} />
                  <Route path="tasks/new" component={TaskPage} />
                  <Route path="sources/new" component={SourcePage} />
                  <Route path="data-explorer" component={DataExplorerPage} />
                  <Route path="dashboards" component={DashboardsPage} />
                  <Route path="manage-sources" component={ManageSources} />
                  <Route path="manage-sources/new" component={SourcePage} />
                  <Route
                    path="manage-sources/:id/edit"
                    component={SourcePage}
                  />
                  <Route path="user/:tab" component={UserPage} />
                  <Route path="logs" component={LogsPage} />
                </Route>
              </Route>
            </Route>
          </Route>
          <Route path="*" component={NotFound} />
        </Router>
      </Provider>
    ) : (
      <div className="page-spinner" />
    )
  }
}

if (rootNode) {
  render(<Root />, rootNode)
}
