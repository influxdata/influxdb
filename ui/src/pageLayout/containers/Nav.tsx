// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

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
          path={`${orgPrefix}/me`}
          icon={IconFont.CuboNav}
          active={this.activateNavItem(['me', 'account'])}
        >
          <NavMenu.SubItem title="Logout" path="/logout" highlightPaths={[]} />
        </NavMenu.Item>
        <NavMenu.Item
          title="Data Explorer"
          path={`${orgPrefix}/data-explorer`}
          icon={IconFont.GraphLine}
          active={this.activateNavItem(['data-explorer'])}
        />
        <NavMenu.Item
          title="Dashboards"
          path={`${orgPrefix}/dashboards`}
          icon={IconFont.Dashboards}
          active={this.activateNavItem(['dashboards'])}
        />
        <NavMenu.Item
          title="Tasks"
          path={`${orgPrefix}/tasks`}
          icon={IconFont.Calendar}
          active={this.activateNavItem(['tasks'])}
        />
        <NavMenu.Item
          title="Settings"
          path={`${orgPrefix}/settings`}
          icon={IconFont.Wrench}
          active={this.activateNavItem(['settings'])}
        >
          <NavMenu.SubItem
            title="Profile"
            path={`${orgPrefix}/configuration/settings_tab`}
            active={this.activateNavItem(['settings_tab'])}
          />
          <NavMenu.SubItem
            title="Tokens"
            path={`${orgPrefix}/configuration/tokens_tab`}
            active={this.activateNavItem(['tokens_tab'])}
          />
        </NavMenu.Item>
        <CloudNav />
      </NavMenu>
    )
  }

  private activateNavItem = (keywords: string[]): boolean => {
    const parentPath = _.get(location.pathname.split('/'), '3', '')
    return keywords.some(path => path === parentPath)
  }
}

const mstp = (state: AppState) => {
  const isHidden = state.app.ephemeral.inPresentationMode
  const {me} = state

  return {isHidden, me}
}

export default connect(mstp)(withRouter<OwnProps>(SideNav))
