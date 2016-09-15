import React, {PropTypes} from 'react';
import {render} from 'react-dom';
import {Provider} from 'react-redux';
import {Router, Route, browserHistory} from 'react-router';

import App from 'src/App';
import CheckDataNodes from 'src/CheckDataNodes';
import OverviewPage from 'src/overview';
import QueriesPage from 'src/queries';
import TasksPage from 'src/tasks';
import RetentionPoliciesPage from 'src/retention_policies';
import DataExplorer from 'src/chronograf';
import DatabaseManager from 'src/database_manager';
import SignUp from 'src/sign_up';
import {UsersPage, UserEditPage} from 'src/web_users';
import {ClusterAccountsPage, ClusterAccountPage} from 'src/cluster_accounts';
import {RolesPageContainer, RolePageContainer} from 'src/access_control';
import AccountSettingsPage from 'src/account_settings';
import NotFound from 'src/shared/components/NotFound';
import InsufficientPermissions from 'src/shared/components/InsufficientPermissions';
import NoClusterError from 'src/shared/components/NoClusterError';
import configureStore from 'src/store/configureStore';
import {webUserShape} from 'src/utils/propTypes';
import {meShow, getClusterAccount, getRoles} from 'shared/apis';
import {buildClusterAccounts} from 'src/shared/presenters';

import 'src/style/enterprise_style/application.scss';

const VIEW_CHRONOGRAF = 'ViewChronograf';
const READ = 'ReadData';

const defaultTimeRange = {upper: null, lower: 'now() - 15m'};
const lsTimeRange = window.localStorage.getItem('timeRange');
const parsedTimeRange = JSON.parse(lsTimeRange) || {};
const timeRange = Object.assign(defaultTimeRange, parsedTimeRange);

const store = configureStore({timeRange});
const rootNode = document.getElementById('react-root');

function hasPermission(account, permission) {
  const hasNakedPermission = !!account.permissions.find(p => p.name === permission);
  const hasPermissionInRole = account.roles.some((role) => {
    return role.permissions.some(p => p.name === permission);
  });

  return hasNakedPermission || hasPermissionInRole;
}

const HTTP_SERVER_ERROR = 500;

const Root = React.createClass({
  getInitialState() {
    return {
      me: null,
      canViewChronograf: false,
      isFetching: true,
      hasReadPermission: false,
      clusterStatus: null,
    };
  },

  componentDidMount() {
    meShow().then(({data: me}) => {
      const match = window.location.pathname.match(/\/clusters\/(\d*)/);
      const clusterID = match && match[1];
      const clusterLink = me.cluster_links.find(link => link.cluster_id === clusterID);
      if (clusterLink) {
        Promise.all([
          getClusterAccount(clusterID, clusterLink.cluster_user),
          getRoles(clusterID),
        ]).then(([{data: {users}}, {data: {roles}}]) => {
          const account = buildClusterAccounts(users, roles)[0];
          const canViewChronograf = hasPermission(account, VIEW_CHRONOGRAF);
          const hasReadPermission = hasPermission(account, READ);
          this.setState({
            me,
            canViewChronograf,
            isFetching: false,
            hasReadPermission,
          });
        }).catch((err) => {
          console.error(err); // eslint-disable-line no-console
          this.setState({
            canViewChronograf: false,
            isFetching: false,
            clusterStatus: err.response.status,
          });
        });
      } else {
        this.setState({
          me,
          isFetching: false,
        });
      }
    }).catch((err) => {
      console.error(err); // eslint-disable-line no-console
      this.setState({
        isFetching: false,
      });
    });
  },

  childContextTypes: {
    me: webUserShape,
    canViewChronograf: PropTypes.bool,
  },

  getChildContext() {
    return {
      me: this.state.me,
      canViewChronograf: this.state.canViewChronograf,
    };
  },

  render() {
    const {me, canViewChronograf} = this.state;
    const isAdmin = me && me.admin;

    if (this.state.isFetching) {
      return null;
    }

    if (this.state.clusterStatus === HTTP_SERVER_ERROR) {
      return <NoClusterError />;
    }

    if (me && !this.state.hasReadPermission) {
      return <InsufficientPermissions />;
    }

    return (
      <Provider store={store}>
        <Router history={browserHistory}>
          <Route path="/signup/admin/:step" component={SignUp} />
          <Route path="/clusters/:clusterID" component={App}>
            <Route component={CheckDataNodes}>
              <Route path="overview" component={OverviewPage} />
              <Route path="queries" component={QueriesPage} />
              {isAdmin ? <Route path="accounts" component={ClusterAccountsPage} /> : null}
              {isAdmin ? <Route path="accounts/:accountID" component={ClusterAccountPage} /> : null}
              {isAdmin ? <Route path="databases/manager/:database" component={DatabaseManager} /> : null}
              <Route path="databases/retentionpolicies/:database" component={RetentionPoliciesPage} />
              {canViewChronograf ? <Route path="chronograf/data_explorer" component={DataExplorer} /> : null}
              {canViewChronograf ? <Route path="chronograf/data_explorer/:explorerID" component={DataExplorer} /> : null}
              <Route path="roles" component={RolesPageContainer} />
              <Route path="roles/:roleSlug" component={RolePageContainer} />
            </Route>
            {isAdmin ? <Route path="users" component={UsersPage} /> : null}
            {isAdmin ? <Route path="users/:userID" component={UserEditPage} /> : null}
            <Route path="tasks" component={TasksPage} />
            <Route path="account/settings" component={AccountSettingsPage} />
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
