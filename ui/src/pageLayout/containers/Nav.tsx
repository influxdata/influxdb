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
import {Organization} from '@influxdata/influx'

import {ErrorHandling} from 'src/shared/decorators/errors'
import AccountNavSubItem from 'src/pageLayout/components/AccountNavSubItem'

interface StateProps {
  isHidden: boolean
  me: AppState['me']
  orgs: Organization[]
}

interface State {
  showOrganizations: boolean
}

type Props = StateProps & WithRouterProps

@ErrorHandling
class SideNav extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      showOrganizations: false,
    }
  }

  public render() {
    const {
      isHidden,
      me,
      params: {orgID},
      orgs,
    } = this.props
    if (isHidden) {
      return null
    }

    const orgPrefix = `/orgs/${orgID}`

    return (
      <NavMenu>
        <NavMenu.Item
          title={`${me.name} (${this.orgName})`}
          path={`${orgPrefix}`}
          icon={IconFont.CuboNav}
          active={getNavItemActivation(['me', 'account'], location.pathname)}
        >
          <AccountNavSubItem
            orgs={orgs}
            showOrganizations={this.state.showOrganizations}
            toggleOrganizationsView={this.toggleOrganizationsView}
          />
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
        />
        <CloudNav />
      </NavMenu>
    )
  }

  private get orgName(): string {
    const {
      params: {orgID},
      orgs,
    } = this.props
    return orgs.find(org => {
      return org.id === orgID
    }).name
  }

  private toggleOrganizationsView = (): void => {
    this.setState({showOrganizations: !this.state.showOrganizations})
  }
}

const mstp = (state: AppState): StateProps => {
  const isHidden = state.app.ephemeral.inPresentationMode
  const {me, orgs} = state

  return {isHidden, me, orgs: orgs.items}
}

export default connect<StateProps>(mstp)(withRouter(SideNav))
