import React, {PropTypes} from 'react';
import {NavBar, NavBlock, NavHeader, NavListItem} from 'src/side_nav/components/NavItems';

const {string, shape} = PropTypes;
const SideNav = React.createClass({
  propTypes: {
    location: string.isRequired,
    sourceID: string.isRequired,
    explorationID: string,
    me: shape({
      email: string,
    }),
  },

  render() {
    const {me, location, sourceID, explorationID} = this.props;
    const sourcePrefix = `/sources/${sourceID}`;
    const explorationSuffix = explorationID ? `/${explorationID}` : '';
    const dataExplorerLink = `${sourcePrefix}/chronograf/data-explorer${explorationSuffix}`;

    const loggedIn = !!(me && me.email);

    return (
      <NavBar location={location}>
        <div className="sidebar__logo">
          <a href="/"><span className="icon cubo-uniform"></span></a>
        </div>
        <NavBlock icon="cpu" link={`${sourcePrefix}/hosts`}>
          <NavHeader link={`${sourcePrefix}/hosts`} title="Infrastructure" />
          <NavListItem link={`${sourcePrefix}/hosts`}>Host List</NavListItem>
          <NavListItem link={`${sourcePrefix}/kubernetes`}>Kubernetes Dashboard</NavListItem>
        </NavBlock>
        <NavBlock icon="graphline" link={dataExplorerLink}>
          <NavHeader link={dataExplorerLink} title={'Data'} />
          <NavListItem link={dataExplorerLink}>Explorer</NavListItem>
          <NavListItem link={`${sourcePrefix}/dashboards`}>Dashboards</NavListItem>
        </NavBlock>
        <NavBlock matcher="alerts" icon="pulse-b" link={`${sourcePrefix}/alerts`}>
          <NavHeader link={`${sourcePrefix}/alerts`} title="Alerting" />
          <NavListItem link={`${sourcePrefix}/alerts`}>Alert History</NavListItem>
          <NavListItem link={`${sourcePrefix}/alert-rules`}>Kapacitor Rules</NavListItem>
        </NavBlock>
        <NavBlock icon="access-key" link={`${sourcePrefix}/manage-sources`}>
          <NavHeader link={`${sourcePrefix}/manage-sources`} title="Configuration" />
          <NavListItem link={`${sourcePrefix}/manage-sources`}>InfluxDB</NavListItem>
          <NavListItem link={`${sourcePrefix}/kapacitor-config`}>Kapacitor</NavListItem>
        </NavBlock>
        {loggedIn ? (
        <NavBlock icon="user-outline" className="sidebar__square-last">
          <a className="sidebar__menu-item" href="/oauth/logout">Logout</a>
        </NavBlock>
        ) : null}
      </NavBar>
    );
  },
});

export default SideNav;
