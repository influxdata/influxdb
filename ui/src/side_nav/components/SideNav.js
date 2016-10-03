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
          <a href="/"><span className="icon cubo-uniform"></span></a>
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
          <NavListItem matcher="manage-sources$" link={`${sourcePrefix}/manage-sources`}>Manage Sources</NavListItem>
          <NavListItem matcher="queries" link={`${sourcePrefix}/queries`}>Queries</NavListItem>
          <NavListItem matcher="tasks" link={`${sourcePrefix}/tasks`}>Tasks</NavListItem>
          <NavListItem matcher="roles" link={`${sourcePrefix}/roles`}>Roles</NavListItem>
          <NavListItem matcher="accounts" link={`${sourcePrefix}/accounts`}>Cluster Accounts</NavListItem>
          <NavListItem matcher="manager" link={`${sourcePrefix}/databases/manager/_internal`}>Database Manager</NavListItem>
          <NavListItem matcher="retentionpolicies" link={`${sourcePrefix}/databases/retentionpolicies/_internal`}>Retention Policies</NavListItem>
        </NavBlock>
        <NavBlock matcher="alerting" icon="cubo-uniform">
          <NavListItem matcher="kapacitor" link={`${sourcePrefix}/kapacitor`}>Kapacitor Config</NavListItem>
          <NavListItem matcher="kapacitor_tasks" link={`${sourcePrefix}/kapacitor_tasks`}>Kapacitor Tasks</NavListItem>
        </NavBlock>
      </NavBar>
    );
  },
});

export default SideNav;
