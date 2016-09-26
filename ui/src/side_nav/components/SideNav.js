import React, {PropTypes} from 'react';
import {NavBar, NavBlock, NavHeader, NavListItem} from 'src/side_nav/components/NavItems';

const {string} = PropTypes;
const SideNav = React.createClass({
  propTypes: {
    location: string.isRequired,
    sourceID: string.isRequired,
  },

  render() {
    const {location, sourceID} = this.props;
    const sourcePrefix = `/sources/${sourceID}`;

    return (
      <NavBar location={location}>
        <div className="sidebar__logo">
          <span className="icon cubo-uniform"></span>
        </div>
        <NavBlock matcher={'users'} icon={"access-key"} link={`${sourcePrefix}/users`}>
          <NavHeader link={`${sourcePrefix}/users`} title="Web Admin" />
          <NavListItem matcher={'users'} link={`${sourcePrefix}/users`}>Users</NavListItem>
        </NavBlock>
        <NavBlock matcher="overview" icon="crown" link={`${sourcePrefix}/overview`}>
          <NavHeader link={`${sourcePrefix}/overview`} title="Cluster" />
          <NavListItem matcher="overview" link={`${sourcePrefix}/overview`}>Overview</NavListItem>
          <NavListItem matcher="queries" link={`${sourcePrefix}/queries`}>Queries</NavListItem>
          <NavListItem matcher="tasks" link={`${sourcePrefix}/tasks`}>Tasks</NavListItem>
          <NavListItem matcher="roles" link={`${sourcePrefix}/roles`}>Roles</NavListItem>
          <NavListItem matcher="accounts" link={`${sourcePrefix}/accounts`}>Cluster Accounts</NavListItem>
          <NavListItem matcher="manager" link={`${sourcePrefix}/databases/manager/_internal`}>Database Manager</NavListItem>
          <NavListItem matcher="retentionpolicies" link={`${sourcePrefix}/databases/retentionpolicies/_internal`}>Retention Policies</NavListItem>
        </NavBlock>
        <NavBlock matcher="chronograf" icon="graphline" link={`${sourcePrefix}/chronograf/data_explorer`}>
          <NavHeader link={`${sourcePrefix}/chronograf/data_explorer`} title={'Chronograf'} />
          <NavListItem matcher={'data_explorer'} link={`${sourcePrefix}/chronograf/data_explorer`}>Data Explorer</NavListItem>
        </NavBlock>
        <NavBlock matcher="settings" icon="user-outline" link={`${sourcePrefix}/account/settings`}>
          <NavHeader link={`${sourcePrefix}/account/settings`} title="My Account" />
          <NavListItem matcher="settings" link={`${sourcePrefix}/account/settings`}>Settings</NavListItem>
          <a className="sidebar__menu-item" href="/logout">Logout</a>
        </NavBlock>
        <NavBlock matcher="hosts" icon="cpu" link={`${sourcePrefix}/hosts`}>
          <NavHeader link={`${sourcePrefix}/hosts`} title="Infrastructure" />
          <NavListItem matcher="hosts" link={`${sourcePrefix}/hosts`}>Host List</NavListItem>
        </NavBlock>
      </NavBar>
    );
  },
});

export default SideNav;
