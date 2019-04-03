// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import NavMenu from 'src/pageLayout/components/NavMenu'
import CloudNav from 'src/pageLayout/components/CloudNav'

// Utils
import {getNavItemActivation} from 'src/pageLayout/utils'

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
          path={`${orgPrefix}/me`}
          icon={IconFont.CuboNav}
          active={getNavItemActivation(['me', 'account'], location.pathname)}
        >
          <NavMenu.SubItem title="Logout" path="/logout" active={false} />
        </NavMenu.Item>
        <NavMenu.Item
          title="Data Explorer"
          path={`${orgPrefix}/data-explorer`}
          icon={IconFont.GraphLine}
          active={getNavItemActivation(['data-explorer'], location.pathname)}
        />
        <NavMenu.Item
          title="Dashboards"
          path={`${orgPrefix}/dashboards`}
          icon={IconFont.Dashboards}
          active={getNavItemActivation(['dashboards'], location.pathname)}
        />
        <NavMenu.Item
          title="Tasks"
          path={`${orgPrefix}/tasks`}
          icon={IconFont.Calendar}
          active={getNavItemActivation(['tasks'], location.pathname)}
        />
        <NavMenu.Item
          title="Settings"
          path={`${orgPrefix}/settings`}
          icon={IconFont.Wrench}
          active={getNavItemActivation(['settings'], location.pathname)}
        >
          <NavMenu.SubItem
            title="Profile"
            path={`${orgPrefix}/configuration/settings_tab`}
            active={getNavItemActivation(['settings_tab'], location.pathname)}
          />
          <NavMenu.SubItem
            title="Tokens"
            path={`${orgPrefix}/configuration/tokens_tab`}
            active={getNavItemActivation(['tokens_tab'], location.pathname)}
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
