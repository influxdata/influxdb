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
        </NavBlock>
        <NavBlock matcher="alerts" icon="alert-triangle" link={`${sourcePrefix}/alerts`}>
          <NavHeader link={`${sourcePrefix}/alerts`} title="Alerts" />
          <NavListItem link={`${sourcePrefix}/alerts`}>View</NavListItem>
          <NavListItem link={`${sourcePrefix}/alert-rules`}>Rules</NavListItem>
        </NavBlock>
      </NavBar>
    );
  },
});

export default SideNav;
