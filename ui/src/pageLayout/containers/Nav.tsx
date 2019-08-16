// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps, Link} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {NavMenu, Icon} from '@influxdata/clockface'
import CloudNav from 'src/pageLayout/components/CloudNav'
import AccountNavSubItem from 'src/pageLayout/components/AccountNavSubItem'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Utils
import {getNavItemActivation} from 'src/pageLayout/utils'

// Types
import {AppState, Organization} from 'src/types'
import {IconFont} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface StateProps {
  isHidden: boolean
  me: AppState['me']
  orgs: Organization[]
  orgName: string
}

interface State {
  isShowingOrganizations: boolean
}

type Props = StateProps & WithRouterProps

@ErrorHandling
class SideNav extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isShowingOrganizations: false,
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
    const dashboardsLink = `${orgPrefix}/dashboards`
    const dataExplorerLink = `${orgPrefix}/data-explorer`
    const tasksLink = `${orgPrefix}/tasks`
    const alertingLink = `${orgPrefix}/alerting`
    const alertHistoryLink = `${orgPrefix}/alert-history`
    const settingsLink = `${orgPrefix}/settings`
    const feedbackLink =
      'https://docs.google.com/forms/d/e/1FAIpQLSdGJpnIZGotN1VFJPkgZEhrt4t4f6QY1lMgMSRUnMeN3FjCKA/viewform?usp=sf_link'

    return (
      <NavMenu>
        <div onMouseLeave={this.closeOrganizationsView} className="find-me">
          <NavMenu.Item
            titleLink={className => (
              <Link className={className} to={orgPrefix}>
                <CloudOnly>{me.name}</CloudOnly>
                <CloudExclude>{`${me.name} (${orgName})`}</CloudExclude>
              </Link>
            )}
            iconLink={className => (
              <Link to={orgPrefix} className={className}>
                <Icon glyph={IconFont.CuboNav} />
              </Link>
            )}
            active={getNavItemActivation(['me', 'account'], location.pathname)}
          >
            <AccountNavSubItem
              orgs={orgs}
              isShowingOrganizations={this.state.isShowingOrganizations}
              showOrganizationsView={this.showOrganizationsView}
              closeOrganizationsView={this.closeOrganizationsView}
            />
          </NavMenu.Item>
        </div>
        <NavMenu.Item
          titleLink={className => (
            <Link className={className} to={dataExplorerLink}>
              Data Explorer
            </Link>
          )}
          iconLink={className => (
            <Link to={dataExplorerLink} className={className}>
              <Icon glyph={IconFont.GraphLine} />
            </Link>
          )}
          active={getNavItemActivation(['data-explorer'], location.pathname)}
        />
        <NavMenu.Item
          titleLink={className => (
            <Link className={className} to={dashboardsLink}>
              Dashboards
            </Link>
          )}
          iconLink={className => (
            <Link to={dashboardsLink} className={className}>
              <Icon glyph={IconFont.Dashboards} />
            </Link>
          )}
          active={getNavItemActivation(['dashboards'], location.pathname)}
        />
        <NavMenu.Item
          titleLink={className => (
            <Link className={className} to={tasksLink}>
              Tasks
            </Link>
          )}
          iconLink={className => (
            <Link to={tasksLink} className={className}>
              <Icon glyph={IconFont.Calendar} />
            </Link>
          )}
          active={getNavItemActivation(['tasks'], location.pathname)}
        />
        <FeatureFlag name="alerting">
          <NavMenu.Item
            titleLink={className => (
              <Link className={className} to={alertingLink}>
                Monitoring & Alerting
              </Link>
            )}
            iconLink={className => (
              <Link to={alertingLink} className={className}>
                <Icon glyph={IconFont.Bell} />
              </Link>
            )}
            active={getNavItemActivation(['alerting'], location.pathname)}
          >
            <NavMenu.SubItem
              titleLink={className => (
                <Link to={alertHistoryLink} className={className}>
                  Check Statuses
                </Link>
              )}
              active={false}
              key="alert-history"
            />
          </NavMenu.Item>
        </FeatureFlag>
        <NavMenu.Item
          titleLink={className => (
            <Link className={className} to={settingsLink}>
              Settings
            </Link>
          )}
          iconLink={className => (
            <Link to={settingsLink} className={className}>
              <Icon glyph={IconFont.Wrench} />
            </Link>
          )}
          active={getNavItemActivation(['settings'], location.pathname)}
        />
        <CloudNav />
        <NavMenu.Item
          titleLink={className => (
            <a className={className} href={feedbackLink} target="_blank">
              Feedback
            </a>
          )}
          iconLink={className => (
            <a href={feedbackLink} className={className} target="_blank">
              <Icon glyph={IconFont.NavChat} />
            </a>
          )}
          active={getNavItemActivation(['feedback'], location.pathname)}
        />
      </NavMenu>
    )
  }

  private showOrganizationsView = (): void => {
    this.setState({isShowingOrganizations: true})
  }

  private closeOrganizationsView = (): void => {
    this.setState({isShowingOrganizations: false})
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
