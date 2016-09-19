import React, {PropTypes} from 'react';
import RenameClusterModal from '../modals/RenameCluster';
import {NavBar, NavBlock, NavHeader, NavListItem} from 'src/side_nav/components/NavItems';

const {func, string, arrayOf, shape, bool} = PropTypes;
const SideNav = React.createClass({
  propTypes: {
    isAdmin: bool.isRequired,
    canViewChronograf: bool.isRequired,
    clusterID: string.isRequired,
    location: string.isRequired,
    onRenameCluster: func.isRequired,
    onOpenRenameModal: func.isRequired,
    clusters: arrayOf(shape()).isRequired,
  },

  handleOpenRenameModal(clusterID) {
    this.props.onOpenRenameModal(clusterID);
  },

  render() {
    const {clusterID, location, onRenameCluster, clusters, isAdmin, canViewChronograf} = this.props;

    return (
      <NavBar location={location}>
        <div className="sidebar__logo">
          <span className="icon cubo-uniform"></span>
        </div>
        {
          isAdmin ?
            <NavBlock matcher={'users'} icon={"access-key"} link={`/clusters/${clusterID}/users`}>
              <NavHeader link={`/clusters/${clusterID}/users`} title="Web Admin" />
              <NavListItem matcher={'users'} link={`/clusters/${clusterID}/users`}>Users</NavListItem>
            </NavBlock>
          : null
        }
        <NavBlock matcher="overview" icon="crown" link={`/clusters/${clusterID}/overview`}>
          <NavHeader link={`/clusters/${clusterID}/overview`} title="Cluster" />
          <NavListItem matcher="overview" link={`/clusters/${clusterID}/overview`}>Overview</NavListItem>
          <NavListItem matcher="queries" link={`/clusters/${clusterID}/queries`}>Queries</NavListItem>
          <NavListItem matcher="tasks" link={`/clusters/${clusterID}/tasks`}>Tasks</NavListItem>
          <NavListItem matcher="roles" link={`/clusters/${clusterID}/roles`}>Roles</NavListItem>
          {isAdmin ? <NavListItem matcher="accounts" link={`/clusters/${clusterID}/accounts`}>Cluster Accounts</NavListItem> : null}
          {isAdmin ? <NavListItem matcher="manager" link={`/clusters/${clusterID}/databases/manager/_internal`}>Database Manager</NavListItem> : null}
          <NavListItem matcher="retentionpolicies" link={`/clusters/${clusterID}/databases/retentionpolicies/_internal`}>Retention Policies</NavListItem>
        </NavBlock>
        {
          canViewChronograf ?
            <NavBlock matcher="chronograf" icon="graphline" link={`/clusters/${clusterID}/chronograf/data_explorer`}>
              <NavHeader link={`/clusters/${clusterID}/chronograf/data_explorer`} title={'Chronograf'} />
              <NavListItem matcher={'data_explorer'} link={`/clusters/${clusterID}/chronograf/data_explorer`}>Data Explorer</NavListItem>
            </NavBlock>
          : null
        }
        <NavBlock matcher="settings" icon="user-outline" link={`/clusters/${clusterID}/account/settings`}>
          <NavHeader link={`/clusters/${clusterID}/account/settings`} title="My Account" />
          <NavListItem matcher="settings" link={`/clusters/${clusterID}/account/settings`}>Settings</NavListItem>
          <a className="sidebar__menu-item" href="/logout">Logout</a>
        </NavBlock>
        <NavBlock icon="server" className="sidebar__square-last" wrapperClassName="sidebar__menu-wrapper-bottom">
          {
            clusters.map(({cluster_id: id, display_name: displayName}, i) => {
              return (
                <NavListItem key={i} matcher={id} link={`/clusters/${id}/overview`}>
                  {displayName || id}
                  {
                    isAdmin ?
                      <button className="btn btn-sm js-open-rename-cluster-modal" onClick={() => this.handleOpenRenameModal(id)} data-toggle="modal" data-target="#rename-cluster-modal">
                        <span className="icon pencil"></span>
                      </button>
                    : null
                  }
                </NavListItem>
              );
            })
          }
        </NavBlock>
        {isAdmin ? <RenameClusterModal clusterID={clusterID} onRenameCluster={onRenameCluster}/> : null}
      </NavBar>
    );
  },
});

export default SideNav;
