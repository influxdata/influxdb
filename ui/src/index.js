import React from 'react';
import {render} from 'react-dom';
import {Provider} from 'react-redux';
import {Router, Route, browserHistory} from 'react-router';

import App from 'src/App';
import AlertsApp from 'src/alerts';
import CheckSources from 'src/CheckSources';
import {HostsPage, HostPage} from 'src/hosts';
import {KubernetesPage} from 'src/kubernetes';
import {CheckAuth, Login} from 'src/auth';
import {KapacitorPage, KapacitorRulePage, KapacitorRulesPage, KapacitorTasksPage} from 'src/kapacitor';
import DataExplorer from 'src/chronograf';
import {CreateSource, SourceForm, ManageSources} from 'src/sources';
import NotFound from 'src/shared/components/NotFound';
import configureStore from 'src/store/configureStore';
import {getSources} from 'shared/apis';

import 'src/style/enterprise_style/application.scss';

const defaultTimeRange = {upper: null, lower: 'now() - 15m'};
const lsTimeRange = window.localStorage.getItem('timeRange');
const parsedTimeRange = JSON.parse(lsTimeRange) || {};
const timeRange = Object.assign(defaultTimeRange, parsedTimeRange);

const store = configureStore({timeRange});
const rootNode = document.getElementById('react-root');

const Root = React.createClass({
  activeSource(sources) {
    const defaultSource = sources.find((s) => s.default);
    if (defaultSource && defaultSource.id) {
      return defaultSource;
    }
    return sources[0];
  },

  redirectToHosts(_, replace, callback) {
    getSources().then(({data: {sources}}) => {
      if (sources && sources.length) {
        const path = `/sources/${this.activeSource(sources).id}/hosts`;
        replace(path);
      }
      callback();
    }).catch(callback);
  },

  render() {
    return (
      <Provider store={store}>
        <Router history={browserHistory}>
          <Route path="/login" component={Login} />
          <Route component={CheckAuth}>
            <Route path="/" component={CreateSource} onEnter={this.redirectToHosts} />
            <Route path="/sources/:sourceID" component={App}>
              <Route component={CheckSources}>
                <Route path="manage-sources" component={ManageSources} />
                <Route path="manage-sources/new" component={SourceForm} />
                <Route path="manage-sources/:id/edit" component={SourceForm} />
                <Route path="chronograf/data-explorer" component={DataExplorer} />
                <Route path="chronograf/data-explorer/:base64ExplorerID" component={DataExplorer} />
                <Route path="hosts" component={HostsPage} />
                <Route path="hosts/:hostID" component={HostPage} />
                <Route path="kubernetes" component={KubernetesPage} />
                <Route path="kapacitor-config" component={KapacitorPage} />
                <Route path="kapacitor-tasks" component={KapacitorTasksPage} />
                <Route path="alerts" component={AlertsApp} />
                <Route path="alert-rules" component={KapacitorRulesPage} />
                <Route path="alert-rules/:ruleID" component={KapacitorRulePage} />
                <Route path="alert-rules/new" component={KapacitorRulePage} />
              </Route>
              <Route path="*" component={NotFound} />
            </Route>
          </Route>
        </Router>
      </Provider>
    );
  },
});

if (rootNode) {
  render(<Root />, rootNode);
}
