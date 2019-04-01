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
    if (isHidden) {
      return null
    }

    const orgPrefix = `/orgs/${orgID}`

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
            link="/logout"
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
          title="Settings"
          link={`${orgPrefix}/settings`}
          icon={IconFont.Wrench}
          location={location.pathname}
          highlightPaths={['settings']}
        >
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
