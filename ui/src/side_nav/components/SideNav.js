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
        <NavBlock matcher="hosts" icon="cpu" link={`${sourcePrefix}/hosts`}>
          <NavHeader link={`${sourcePrefix}/hosts`} title="Infrastructure" />
          <NavListItem matcher="hosts" link={`${sourcePrefix}/hosts`}>Host List</NavListItem>
        </NavBlock>
        <NavBlock matcher="chronograf" icon="graphline" link={`${sourcePrefix}/chronograf/data_explorer`}>
          <NavHeader link={`${sourcePrefix}/chronograf/data_explorer`} title={'Chronograf'} />
          <NavListItem matcher={'data_explorer'} link={`${sourcePrefix}/chronograf/data_explorer`}>Data Explorer</NavListItem>
        </NavBlock>
        <NavBlock matcher="overview" icon="crown" link={`${sourcePrefix}/overview`}>
          <NavHeader link={`${sourcePrefix}/overview`} title="Sources" />
          <NavListItem matcher="overview" link={`${sourcePrefix}/overview`}>Overview</NavListItem>
          <NavListItem matcher="sources$" link={`/sources`}>Manage Sources</NavListItem>
          <NavListItem matcher="queries" link={`${sourcePrefix}/queries`}>Queries</NavListItem>
          <NavListItem matcher="tasks" link={`${sourcePrefix}/tasks`}>Tasks</NavListItem>
          <NavListItem matcher="roles" link={`${sourcePrefix}/roles`}>Roles</NavListItem>
          <NavListItem matcher="accounts" link={`${sourcePrefix}/accounts`}>Cluster Accounts</NavListItem>
          <NavListItem matcher="manager" link={`${sourcePrefix}/databases/manager/_internal`}>Database Manager</NavListItem>
          <NavListItem matcher="retentionpolicies" link={`${sourcePrefix}/databases/retentionpolicies/_internal`}>Retention Policies</NavListItem>
        </NavBlock>
      </NavBar>
    );
  },
});

export default SideNav;
