import React, {PropTypes} from 'react';
import {render} from 'react-dom';
import {Provider} from 'react-redux';
import {Router, Route, browserHistory} from 'react-router';

import App from 'src/App';
import AlertsApp from 'src/alerts';
import CheckDataNodes from 'src/CheckDataNodes';
import {HostsPage, HostPage} from 'src/hosts';
import {KapacitorPage, KapacitorRulePage, KapacitorRulesPage, KapacitorTasksPage} from 'src/kapacitor';
import QueriesPage from 'src/queries';
import TasksPage from 'src/tasks';
import RetentionPoliciesPage from 'src/retention_policies';
import DataExplorer from 'src/chronograf';
import DatabaseManager from 'src/database_manager';
import SignUp from 'src/sign_up';
import {CreateSource, SourceForm, ManageSources} from 'src/sources';
import {ClusterAccountsPage, ClusterAccountPage} from 'src/cluster_accounts';
import {RolesPageContainer, RolePageContainer} from 'src/access_control';
import NotFound from 'src/shared/components/NotFound';
import NoClusterError from 'src/shared/components/NoClusterError';
import configureStore from 'src/store/configureStore';
import {getSources} from 'shared/apis';

import 'src/style/enterprise_style/application.scss';

const {number, shape, string, bool} = PropTypes;

const defaultTimeRange = {upper: null, lower: 'now() - 15m'};
const lsTimeRange = window.localStorage.getItem('timeRange');
const parsedTimeRange = JSON.parse(lsTimeRange) || {};
const timeRange = Object.assign(defaultTimeRange, parsedTimeRange);

const store = configureStore({timeRange});
const rootNode = document.getElementById('react-root');

const HTTP_SERVER_ERROR = 500;

const Root = React.createClass({
  getInitialState() {
    return {
      me: {
        id: 1,
        name: 'Chronograf',
        email: 'foo@example.com',
        admin: true,
      },
      isFetching: false,
      hasReadPermission: false,
      clusterStatus: null,
    };
  },

  childContextTypes: {
    me: shape({
      id: number.isRequired,
      name: string.isRequired,
      email: string.isRequired,
      admin: bool.isRequired,
    }),
  },

  getChildContext() {
    return {
      me: this.state.me,
    };
  },

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
    if (this.state.isFetching) {
      return null;
    }

    if (this.state.clusterStatus === HTTP_SERVER_ERROR) {
      return <NoClusterError />;
    }

    return (
      <Provider store={store}>
        <Router history={browserHistory}>
          <Route path="/signup/admin/:step" component={SignUp} />
          <Route path="/" component={CreateSource} onEnter={this.redirectToHosts} />
          <Route path="/sources/:sourceID" component={App}>
            <Route component={CheckDataNodes}>
              <Route path="manage-sources" component={ManageSources} />
              <Route path="manage-sources/new" component={SourceForm} />
              <Route path="manage-sources/:id/edit" component={SourceForm} />
              <Route path="queries" component={QueriesPage} />
              <Route path="accounts" component={ClusterAccountsPage} />
              <Route path="accounts/:accountID" component={ClusterAccountPage} />
              <Route path="databases/manager/:database" component={DatabaseManager} />
              <Route path="databases/retentionpolicies/:database" component={RetentionPoliciesPage} />
              <Route path="chronograf/data-explorer" component={DataExplorer} />
              <Route path="chronograf/data-explorer/:base64ExplorerID" component={DataExplorer} />
              <Route path="roles" component={RolesPageContainer} />
              <Route path="roles/:roleSlug" component={RolePageContainer} />
              <Route path="hosts" component={HostsPage} />
              <Route path="hosts/:hostID" component={HostPage} />
              <Route path="kapacitor-config" component={KapacitorPage} />
              <Route path="kapacitor-tasks" component={KapacitorTasksPage} />
              <Route path="alerts" component={AlertsApp} />
              <Route path="alert-rules" component={KapacitorRulesPage} />
              <Route path="alert-rules/:ruleID" component={KapacitorRulePage} />
            </Route>
            <Route path="tasks" component={TasksPage} />
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
