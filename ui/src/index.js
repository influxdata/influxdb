import React from 'react';
import {render} from 'react-dom';
import {Provider} from 'react-redux';
import {Router, Route, browserHistory, Redirect} from 'react-router';

import App from 'src/App';
import AlertsApp from 'src/alerts';
import CheckSources from 'src/CheckSources';
import {HostsPage, HostPage} from 'src/hosts';
import {KubernetesPage} from 'src/kubernetes';
import {Login} from 'src/auth';
import {KapacitorPage, KapacitorRulePage, KapacitorRulesPage, KapacitorTasksPage} from 'src/kapacitor';
import DataExplorer from 'src/data_explorer';
import {DashboardsPage, DashboardPage} from 'src/dashboards';
import {CreateSource, SourcePage, ManageSources} from 'src/sources';
import NotFound from 'src/shared/components/NotFound';
import configureStore from 'src/store/configureStore';
import {getMe, getSources} from 'shared/apis';
import {receiveMe} from 'shared/actions/me';

import 'src/style/chronograf.scss';

const defaultTimeRange = {upper: null, lower: 'now() - 15m'};
const lsTimeRange = window.localStorage.getItem('timeRange');
const parsedTimeRange = JSON.parse(lsTimeRange) || {};
const timeRange = Object.assign(defaultTimeRange, parsedTimeRange);

const store = configureStore({timeRange});
const rootNode = document.getElementById('react-root');

const Root = React.createClass({
  getInitialState() {
    return {
      loggedIn: null,
    };
  },
  componentDidMount() {
    this.checkAuth();
  },
  activeSource(sources) {
    const defaultSource = sources.find((s) => s.default);
    if (defaultSource && defaultSource.id) {
      return defaultSource;
    }
    return sources[0];
  },

  redirectFromRoot(_, replace, callback) {
    getSources().then(({data: {sources}}) => {
      if (sources && sources.length) {
        const path = `/sources/${this.activeSource(sources).id}/hosts`;
        replace(path);
      }
      callback();
    });
  },

  checkAuth() {
    if (store.getState().me.links) {
      return this.setState({loggedIn: true});
    }
    getMe().then(({data: me}) => {
      store.dispatch(receiveMe(me));
      this.setState({loggedIn: true});
    }).catch((err) => {
      const AUTH_DISABLED = 418;
      if (err.response.status === AUTH_DISABLED) {
        return this.setState({loggedIn: true});
        // Could store a boolean indicating auth is not set up
      }

      this.setState({loggedIn: false});
    });
  },

  render() {
    if (this.state.loggedIn === null) {
      return <div className="page-spinner"></div>;
    }
    if (this.state.loggedIn === false) {
      return (
        <Provider store={store}>
          <Router history={browserHistory}>
            <Route path="/login" component={Login} />
            <Redirect from="*" to="/login" />
          </Router>
        </Provider>
      );
    }
    return (
      <Provider store={store}>
        <Router history={browserHistory}>
          <Route path="/" component={CreateSource} onEnter={this.redirectFromRoot} />
          <Route path="/sources/new" component={CreateSource} />
          <Route path="/sources/:sourceID" component={App}>
            <Route component={CheckSources}>
              <Route path="manage-sources" component={ManageSources} />
              <Route path="manage-sources/new" component={SourcePage} />
              <Route path="manage-sources/:id/edit" component={SourcePage} />
              <Route path="chronograf/data-explorer" component={DataExplorer} />
              <Route path="chronograf/data-explorer/:base64ExplorerID" component={DataExplorer} />
              <Route path="hosts" component={HostsPage} />
              <Route path="hosts/:hostID" component={HostPage} />
              <Route path="kubernetes" component={KubernetesPage} />
              <Route path="kapacitor-config" component={KapacitorPage} />
              <Route path="kapacitor-tasks" component={KapacitorTasksPage} />
              <Route path="alerts" component={AlertsApp} />
              <Route path="dashboards" component={DashboardsPage} />
              <Route path="dashboards/:id" component={DashboardPage} />
              <Route path="alert-rules" component={KapacitorRulesPage} />
              <Route path="alert-rules/:ruleID" component={KapacitorRulePage} />
              <Route path="alert-rules/new" component={KapacitorRulePage} />
            </Route>
            <Route path="*" component={NotFound} />
          </Route>
        </Router>
      </Provider>
    );
  },
});

if (rootNode) {
  render(<Root />, rootNode);
}
