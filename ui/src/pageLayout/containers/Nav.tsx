// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps, Link} from 'react-router'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {NavMenu, Icon} from '@influxdata/clockface'
import AccountNavSubItem from 'src/pageLayout/components/AccountNavSubItem'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Constants
import {
  HOMEPAGE_PATHNAME,
  CLOUD_URL,
  CLOUD_USERS_PATH,
} from 'src/shared/constants'

// Utils
import {getNavItemActivation} from 'src/pageLayout/utils'

// Types
import {AppState, Organization, ResourceType} from 'src/types'
import {IconFont} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {getAll} from 'src/resources/selectors'

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

    // Home page
    const orgPrefix = `/orgs/${orgID}`
    // Top level nav links
    const dataExplorerLink = `${orgPrefix}/data-explorer`
    const dashboardsLink = `${orgPrefix}/dashboards`
    const tasksLink = `${orgPrefix}/tasks`
    const alertingLink = `${orgPrefix}/alerting`
    const alertHistoryLink = `${orgPrefix}/alert-history`
    // Load data
    const loadDataLink = `${orgPrefix}/load-data/buckets`
    const bucketsLink = `${orgPrefix}/load-data/buckets`
    const telegrafsLink = `${orgPrefix}/load-data/telegrafs`
    const scrapersLink = `${orgPrefix}/load-data/scrapers`
    const tokensLink = `${orgPrefix}/load-data/tokens`
    const clientLibrariesLink = `${orgPrefix}/load-data/client-libraries`
    // Settings
    const settingsLink = `${orgPrefix}/settings`
    const membersLink = `${orgPrefix}/settings/members`
    const variablesLink = `${orgPrefix}/settings/variables`
    const templatesLink = `${orgPrefix}/settings/templates`
    const labelsLink = `${orgPrefix}/settings/labels`
    const profileLink = `${orgPrefix}/settings/about`
    // Feedback
    const feedbackLink =
      'https://docs.google.com/forms/d/e/1FAIpQLSdGJpnIZGotN1VFJPkgZEhrt4t4f6QY1lMgMSRUnMeN3FjCKA/viewform?usp=sf_link'

    // Cloud
    const cloudUsersLink = `${CLOUD_URL}/organizations/${orgID}${CLOUD_USERS_PATH}`

    return (
      <NavMenu>
        <div onMouseLeave={this.closeOrganizationsView} className="find-me">
          <NavMenu.Item
            titleLink={className => (
              <Link className={className} to={orgPrefix}>
                <CloudOnly>Getting Started</CloudOnly>
                <CloudExclude>{`${me.name} (${orgName})`}</CloudExclude>
              </Link>
            )}
            iconLink={className => (
              <Link to={orgPrefix} className={className}>
                <Icon glyph={IconFont.CuboNav} />
              </Link>
            )}
            active={getNavItemActivation(
              [HOMEPAGE_PATHNAME, 'account'],
              location.pathname
            )}
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
            <Link
              className={className}
              to={dashboardsLink}
              data-testid="nav-menu_dashboard"
            >
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
                History
              </Link>
            )}
            active={getNavItemActivation(['alert-history'], location.pathname)}
            key="alert-history"
          />
        </NavMenu.Item>
        <NavMenu.Item
          titleLink={className => (
            <Link className={className} to={loadDataLink}>
              Load Data
            </Link>
          )}
          iconLink={className => (
            <Link to={loadDataLink} className={className}>
              <Icon glyph={IconFont.DisksNav} />
            </Link>
          )}
          active={getNavItemActivation(['load-data'], location.pathname)}
        >
          <NavMenu.SubItem
            titleLink={className => (
              <Link to={bucketsLink} className={className}>
                Buckets
              </Link>
            )}
            active={getNavItemActivation(['buckets'], location.pathname)}
            key="buckets"
          />
          <NavMenu.SubItem
            titleLink={className => (
              <Link to={telegrafsLink} className={className}>
                Telegraf
              </Link>
            )}
            active={getNavItemActivation(['telegrafs'], location.pathname)}
            key="telegrafs"
          />
          <CloudExclude>
            <NavMenu.SubItem
              titleLink={className => (
                <Link to={scrapersLink} className={className}>
                  Scrapers
                </Link>
              )}
              active={getNavItemActivation(['scrapers'], location.pathname)}
              key="scrapers"
            />
          </CloudExclude>
          <NavMenu.SubItem
            titleLink={className => (
              <Link to={tokensLink} className={className}>
                Tokens
              </Link>
            )}
            active={getNavItemActivation(['tokens'], location.pathname)}
            key="tokens"
          />
          <NavMenu.SubItem
            titleLink={className => (
              <Link to={clientLibrariesLink} className={className}>
                Client Libraries
              </Link>
            )}
            active={getNavItemActivation(
              ['client-libraries'],
              location.pathname
            )}
            key="client-libraries"
          />
        </NavMenu.Item>
        <FeatureFlag name="multiUser">
          <CloudOnly>
            <NavMenu.Item
              titleLink={className => (
                <a className={className} href={cloudUsersLink}>
                  Organization Members
                </a>
              )}
              iconLink={className => (
                <a href={cloudUsersLink} className={className}>
                  <Icon glyph={IconFont.UsersTrio} />
                </a>
              )}
              active={getNavItemActivation(['users'], location.pathname)}
            />
          </CloudOnly>
        </FeatureFlag>
        <NavMenu.Item
          titleLink={className => (
            <Link className={className} to={settingsLink}>
              Settings
            </Link>
          )}
          iconLink={className => (
            <Link to={settingsLink} className={className}>
              <Icon glyph={IconFont.WrenchNav} />
            </Link>
          )}
          active={getNavItemActivation(['settings'], location.pathname)}
        >
          <CloudExclude>
            <NavMenu.SubItem
              titleLink={className => (
                <Link to={membersLink} className={className}>
                  Members
                </Link>
              )}
              active={getNavItemActivation(['members'], location.pathname)}
              key="members"
            />
          </CloudExclude>
          <NavMenu.SubItem
            titleLink={className => (
              <Link to={variablesLink} className={className}>
                Variables
              </Link>
            )}
            active={getNavItemActivation(['variables'], location.pathname)}
            key="variables"
          />
          <NavMenu.SubItem
            titleLink={className => (
              <Link to={templatesLink} className={className}>
                Templates
              </Link>
            )}
            active={getNavItemActivation(['templates'], location.pathname)}
            key="templates"
          />
          <NavMenu.SubItem
            titleLink={className => (
              <Link to={labelsLink} className={className}>
                Labels
              </Link>
            )}
            active={getNavItemActivation(['labels'], location.pathname)}
            key="labels"
          />
          <NavMenu.SubItem
            titleLink={className => (
              <Link to={profileLink} className={className}>
                Profile
              </Link>
            )}
            active={getNavItemActivation(['about'], location.pathname)}
            key="profile"
          />
        </NavMenu.Item>
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
  const orgs = getAll<Organization>(state, ResourceType.Orgs)
  const org = getOrg(state)
  const {me} = state

  return {isHidden, me, orgs, orgName: get(org, 'name', '')}
}

export default connect<StateProps>(mstp)(withRouter(SideNav))
