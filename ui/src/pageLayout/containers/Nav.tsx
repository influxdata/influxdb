// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import NavMenu from 'src/pageLayout/components/NavMenu'
import CloudNav from 'src/pageLayout/components/CloudNav'

// Types
import {AppState} from 'src/types'
import {IconFont} from 'src/clockface'

import {ErrorHandling} from 'src/shared/decorators/errors'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'

interface OwnProps {
  isHidden: boolean
  me: AppState['me']
}

type Props = OwnProps & WithRouterProps

@ErrorHandling
class SideNav extends PureComponent<Props> {
  public render() {
    const {
      isHidden,
      me,
      params: {orgID},
    } = this.props
    const {location} = this.props
    if (isHidden) {
      return null
    }
    const orgPrefix = `/org/${orgID}`

    return (
      <NavMenu>
        <NavMenu.Item
          title={me.name}
          link={`${orgPrefix}/me`}
          icon={IconFont.CuboNav}
          location={location.pathname}
          highlightPaths={['me', 'account']}
        >
          <NavMenu.SubItem
            title="Logout"
            link={`${orgPrefix}/logout`}
            location={location.pathname}
            highlightPaths={[]}
          />
        </NavMenu.Item>
        <NavMenu.Item
          title="Data Explorer"
          link={`${orgPrefix}/data-explorer`}
          icon={IconFont.GraphLine}
          location={location.pathname}
          highlightPaths={['data-explorer']}
        />
        <NavMenu.Item
          title="Dashboards"
          link={`${orgPrefix}/dashboards`}
          icon={IconFont.Dashboards}
          location={location.pathname}
          highlightPaths={['dashboards']}
        />
        <NavMenu.Item
          title="Tasks"
          link={`${orgPrefix}/tasks`}
          icon={IconFont.Calendar}
          location={location.pathname}
          highlightPaths={['tasks']}
        />
        <NavMenu.Item
          title="Organizations"
          link={`${orgPrefix}/organizations`}
          icon={IconFont.UsersDuo}
          location={location.pathname}
          highlightPaths={['organizations']}
        />
        <NavMenu.Item
          title="Configuration"
          link={`${orgPrefix}/configuration/buckets_tab`}
          icon={IconFont.Wrench}
          location={location.pathname}
          highlightPaths={['configuration']}
        >
          <CloudExclude>
            <NavMenu.SubItem
              title="Buckets"
              link="/configuration/buckets_tab"
              location={location.pathname}
              highlightPaths={['buckets_tab']}
            />
            <NavMenu.SubItem
              title="Telegrafs"
              link="/configuration/telegrafs_tab"
              location={location.pathname}
              highlightPaths={['telegrafs_tab']}
            />
            <NavMenu.SubItem
              title="Scrapers"
              link="/configuration/scrapers_tab"
              location={location.pathname}
              highlightPaths={['scrapers_tab']}
            />
            <NavMenu.SubItem
              title="Variables"
              link="/configuration/variables_tab"
              location={location.pathname}
              highlightPaths={['variables_tab']}
            />
          </CloudExclude>
          <NavMenu.SubItem
            title="Profile"
            link={`${orgPrefix}/configuration/settings_tab`}
            location={location.pathname}
            highlightPaths={['settings_tab']}
          />
          <NavMenu.SubItem
            title="Tokens"
            link={`${orgPrefix}/configuration/tokens_tab`}
            location={location.pathname}
            highlightPaths={['tokens_tab']}
          />
        </NavMenu.Item>
        <CloudNav />
      </NavMenu>
    )
  }
}

const mstp = (state: AppState) => {
  const isHidden = state.app.ephemeral.inPresentationMode
  const {me} = state

  return {isHidden, me}
}

export default connect(mstp)(withRouter<OwnProps>(SideNav))
