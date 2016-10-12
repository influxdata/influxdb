import React, {PropTypes} from 'react';
import {NavBar, NavBlock, NavHeader, NavListItem} from 'src/side_nav/components/NavItems';

const {string} = PropTypes;
const SideNav = React.createClass({
  propTypes: {
    location: string.isRequired,
    sourceID: string.isRequired,
    explorationID: string,
  },

  render() {
    const {location, sourceID, explorationID} = this.props;
    const sourcePrefix = `/sources/${sourceID}`;
    const explorationSuffix = explorationID ? `/${explorationID}` : '';
    const dataExplorerLink = `${sourcePrefix}/chronograf/data-explorer${explorationSuffix}`;

    return (
      <NavBar location={location}>
        <div className="sidebar__logo">
          <a href="/"><span className="icon cubo-uniform"></span></a>
        </div>
        <NavBlock icon="cpu" link={`${sourcePrefix}/hosts`}>
          <NavHeader link={`${sourcePrefix}/hosts`} title="Infrastructure" />
          <NavListItem link={`${sourcePrefix}/hosts`}>Host List</NavListItem>
        </NavBlock>
        <NavBlock icon="graphline" link={dataExplorerLink}>
          <NavHeader link={dataExplorerLink} title={'Chronograf'} />
          <NavListItem link={dataExplorerLink}>Data Explorer</NavListItem>
        </NavBlock>
        <NavBlock icon="crown" link={`${sourcePrefix}/manage-sources`}>
          <NavHeader link={`${sourcePrefix}/manage-sources`} title="Sources" />
          <NavListItem link={`${sourcePrefix}/manage-sources`}>InfluxDB</NavListItem>
          <NavListItem link={`${sourcePrefix}/kapacitor-config`}>Kapacitor</NavListItem>
          <NavListItem link={`${sourcePrefix}/queries`}>Queries</NavListItem>
          <NavListItem link={`${sourcePrefix}/tasks`}>Tasks</NavListItem>
          <NavListItem link={`${sourcePrefix}/roles`}>Roles</NavListItem>
          <NavListItem link={`${sourcePrefix}/accounts`}>Cluster Accounts</NavListItem>
          <NavListItem link={`${sourcePrefix}/databases/manager/_internal`}>Database Manager</NavListItem>
          <NavListItem link={`${sourcePrefix}/databases/retentionpolicies/_internal`}>Retention Policies</NavListItem>
        </NavBlock>
        <NavBlock icon="cubo-uniform" link={`${sourcePrefix}/kapacitor-tasks`}>
          <NavHeader link={`${sourcePrefix}/kapacitor-tasks`} title="Alerting" />
          <NavListItem link={`${sourcePrefix}/kapacitor-tasks`}>Kapacitor Tasks</NavListItem>
        </NavBlock>
        <NavBlock matcher="alerts" icon="alert-triangle" link={`${sourcePrefix}/alerts`}>
          <NavHeader link={`${sourcePrefix}/alerts`} title="Alerts" />
          <NavListItem matcher="view" link={`${sourcePrefix}/alerts`}>View</NavListItem>
          <NavListItem matcher="rules" link={`${sourcePrefix}/alert/rules`}>Rules</NavListItem>
        </NavBlock>
      </NavBar>
    );
  },
});

export default SideNav;
