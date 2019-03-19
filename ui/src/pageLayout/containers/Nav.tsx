// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import NavMenu from 'src/pageLayout/components/NavMenu'

// Types
import {MeState, AppState} from 'src/types/v2'
import {IconFont} from 'src/clockface'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props extends WithRouterProps {
  isHidden: boolean
  me: MeState
}

@ErrorHandling
class SideNav extends PureComponent<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {isHidden, me} = this.props
    const {location} = this.props
    if (isHidden) {
      return null
    }

    return (
      <NavMenu>
        <NavMenu.Item
          title={me.name}
          link="/me"
          icon={IconFont.CuboNav}
          location={location.pathname}
          highlightPaths={['me', 'account']}
        >
          <NavMenu.SubItem
            title="Logout"
            link={`/logout`}
            location={location.pathname}
            highlightPaths={[]}
          />
        </NavMenu.Item>
        <NavMenu.Item
          title="Data Explorer"
          link="/data-explorer"
          icon={IconFont.GraphLine}
          location={location.pathname}
          highlightPaths={['data-explorer']}
        />
        <NavMenu.Item
          title="Dashboards"
          link="/dashboards"
          icon={IconFont.Dashboards}
          location={location.pathname}
          highlightPaths={['dashboards']}
        />
        <NavMenu.Item
          title="Tasks"
          link="/tasks"
          icon={IconFont.Calendar}
          location={location.pathname}
          highlightPaths={['tasks']}
        />
        <NavMenu.Item
          title="Organizations"
          link="/organizations"
          icon={IconFont.UsersDuo}
          location={location.pathname}
          highlightPaths={['organizations']}
        />
        <NavMenu.Item
          title="Configuration"
          link="/configuration/buckets_tab"
          icon={IconFont.Wrench}
          location={location.pathname}
          highlightPaths={['configuration']}
        >
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
            title="Variables"
            link="/configuration/variables_tab"
            location={location.pathname}
            highlightPaths={['variables_tab']}
          />
          <NavMenu.SubItem
            title="Profile"
            link="/configuration/settings_tab"
            location={location.pathname}
            highlightPaths={['settings_tab']}
          />
          <NavMenu.SubItem
            title="Tokens"
            link="/configuration/tokens_tab"
            location={location.pathname}
            highlightPaths={['tokens_tab']}
          />
        </NavMenu.Item>
      </NavMenu>
    )
  }
}

const mstp = (state: AppState) => {
  const isHidden = state.app.ephemeral.inPresentationMode
  const {me} = state

  return {isHidden, me}
}

export default connect(mstp)(withRouter(SideNav))
