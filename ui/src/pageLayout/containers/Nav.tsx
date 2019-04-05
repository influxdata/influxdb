// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import NavMenu from 'src/pageLayout/components/NavMenu'
import CloudNav from 'src/pageLayout/components/CloudNav'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import AccountNavSubItem from 'src/pageLayout/components/AccountNavSubItem'

// Utils
import {getNavItemActivation} from 'src/pageLayout/utils'

// Types
import {AppState} from 'src/types'
import {IconFont} from 'src/clockface'
import {Organization} from '@influxdata/influx'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface StateProps {
  isHidden: boolean
  me: AppState['me']
  orgs: Organization[]
  orgName: string
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
      orgName,
    } = this.props
    if (isHidden) {
      return null
    }

    const orgPrefix = `/orgs/${orgID}`

    return (
      <NavMenu>
        <NavMenu.Item
          title={`${me.name} (${orgName})`}
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
          title={`${orgName} Settings`}
          path={`${orgPrefix}/settings`}
          icon={IconFont.Wrench}
          active={getNavItemActivation(['settings'], location.pathname)}
        >
          <CloudExclude>
            <NavMenu.SubItem
              title="Create Organization"
              path="/orgs/new"
              active={false}
            />
          </CloudExclude>
        </NavMenu.Item>
        <CloudNav />
      </NavMenu>
    )
  }

  private toggleOrganizationsView = (): void => {
    this.setState({showOrganizations: !this.state.showOrganizations})
  }
}

const mstp = (state: AppState): StateProps => {
  const isHidden = state.app.ephemeral.inPresentationMode
  const {
    me,
    orgs,
    orgs: {org},
  } = state

  return {isHidden, me, orgs: orgs.items, orgName: _.get(org, 'name', '')}
}

export default connect<StateProps>(mstp)(withRouter(SideNav))
