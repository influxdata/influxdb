import React, {PropTypes} from 'react';
import {NavBar, NavBlock, NavHeader, NavListItem} from 'src/side_nav/components/NavItems';

const {string} = PropTypes;
const SideNav = React.createClass({
  propTypes: {
    location: string.isRequired,
  },

  render() {
    const {location} = this.props;

    return (
      <NavBar location={location}>
        <div className="sidebar__logo">
          <span className="icon cubo-uniform"></span>
        </div>
        <NavBlock matcher={'users'} icon={"access-key"} link={`/users`}>
          <NavHeader link={`/users`} title="Web Admin" />
          <NavListItem matcher={'users'} link={`/users`}>Users</NavListItem>
        </NavBlock>
        <NavBlock matcher="overview" icon="crown" link={`/overview`}>
          <NavHeader link={`/overview`} title="Cluster" />
          <NavListItem matcher="overview" link={`/overview`}>Overview</NavListItem>
          <NavListItem matcher="queries" link={`/queries`}>Queries</NavListItem>
          <NavListItem matcher="tasks" link={`/tasks`}>Tasks</NavListItem>
          <NavListItem matcher="roles" link={`/roles`}>Roles</NavListItem>
          <NavListItem matcher="accounts" link={`/accounts`}>Cluster Accounts</NavListItem>
          <NavListItem matcher="manager" link={`/databases/manager/_internal`}>Database Manager</NavListItem>
          <NavListItem matcher="retentionpolicies" link={`/databases/retentionpolicies/_internal`}>Retention Policies</NavListItem>
        </NavBlock>
        <NavBlock matcher="chronograf" icon="graphline" link={`/chronograf/data_explorer`}>
          <NavHeader link={`/chronograf/data_explorer`} title={'Chronograf'} />
          <NavListItem matcher={'data_explorer'} link={`/chronograf/data_explorer`}>Data Explorer</NavListItem>
        </NavBlock>
        <NavBlock matcher="settings" icon="user-outline" link={`/account/settings`}>
          <NavHeader link={`/account/settings`} title="My Account" />
          <NavListItem matcher="settings" link={`/account/settings`}>Settings</NavListItem>
          <a className="sidebar__menu-item" href="/logout">Logout</a>
        </NavBlock>
        <NavBlock matcher="hosts" icon="crown" link={`/hosts/list`}>
          <NavHeader link={`/hosts/list`} title="Infrastructure" />
          <NavListItem matcher="list" link={`/list`}>Host List</NavListItem>
        </NavBlock>
      </NavBar>
    );
  },
});

export default SideNav;
