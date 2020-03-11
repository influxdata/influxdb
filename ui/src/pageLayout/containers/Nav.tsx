// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps, Link} from 'react-router'
import {connect} from 'react-redux'

// Components
import {Icon, TreeNav} from '@influxdata/clockface'
import UserWidget from 'src/pageLayout/components/UserWidget'
import NavHeader from 'src/pageLayout/components/NavHeader'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Constants
import {
  CLOUD_URL,
  CLOUD_USERS_PATH,
  CLOUD_USAGE_PATH,
  CLOUD_BILLING_PATH,
} from 'src/shared/constants'

// Utils
import {getNavItemActivation} from 'src/pageLayout/utils'

// Types
import {AppState} from 'src/types'
import {IconFont} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface StateProps {
  isHidden: boolean
}

interface State {
  isShowingOrganizations: boolean
}

interface NavSubItem {
  id: string
  label: string
  link: string
  cloudExclude?: boolean
  cloudOnly?: boolean
  featureFlag?: string
}
interface NavItem {
  id: string
  label: string
  link: string
  icon: IconFont
  cloudExclude?: boolean
  cloudOnly?: boolean
  featureFlag?: string
  menu?: NavSubItem[]
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
      params: {orgID},
    } = this.props

    if (isHidden) {
      return null
    }

    // Home page
    const orgPrefix = `/orgs/${orgID}`

    const navItems: NavItem[] = [
      {
        id: 'load-data',
        icon: IconFont.Disks,
        label: 'Data',
        link: `${orgPrefix}/load-data/buckets`,
        menu: [
          {
            id: 'buckets',
            label: 'Buckets',
            link: `${orgPrefix}/load-data/buckets`,
          },
          {
            id: 'telegrafs',
            label: 'Telegraf',
            link: `${orgPrefix}/load-data/telegrafs`,
          },
          {
            id: 'scrapers',
            label: 'Scrapers',
            link: `${orgPrefix}/load-data/scrapers`,
            cloudExclude: true,
          },
          {
            id: 'tokens',
            label: 'Tokens',
            link: `${orgPrefix}/load-data/tokens`,
          },
          {
            id: 'client-libraries',
            label: 'Tokens',
            link: `${orgPrefix}/load-data/client-libraries`,
          },
        ],
      },
      {
        id: 'data-explorer',
        icon: IconFont.GraphLine,
        label: 'Explore',
        link: `${orgPrefix}/data-explorer`,
      },
      {
        id: 'settings',
        icon: IconFont.UsersDuo,
        label: 'Org',
        link: `${orgPrefix}/settings/members`,
        menu: [
          {
            id: 'members',
            label: 'Members',
            link: `${orgPrefix}/settings/members`,
          },
          {
            id: 'multi-user-members',
            label: 'Org Members',
            featureFlag: 'multiUser',
            link: `${CLOUD_URL}/organizations/${orgID}/${CLOUD_USERS_PATH}`,
          },
          {
            id: 'usage',
            label: 'Usage',
            link: CLOUD_USAGE_PATH,
            cloudOnly: true,
          },
          {
            id: 'billing',
            label: 'Billing',
            link: CLOUD_BILLING_PATH,
            cloudOnly: true,
          },
          {
            id: 'profile',
            label: 'Profile',
            link: `${orgPrefix}/settings/profile`,
          },
          {
            id: 'variables',
            label: 'Variables',
            link: `${orgPrefix}/settings/variables`,
          },
          {
            id: 'templates',
            label: 'Templates',
            link: `${orgPrefix}/settings/templates`,
          },
          {
            id: 'labels',
            label: 'Labels',
            link: `${orgPrefix}/settings/labels`,
          },
        ],
      },
      {
        id: 'dashboards',
        icon: IconFont.Dashboards,
        label: 'Boards',
        link: `${orgPrefix}/dashboards`,
      },
      {
        id: 'tasks',
        icon: IconFont.Calendar,
        label: 'Tasks',
        link: `${orgPrefix}/tasks`,
      },
      {
        id: 'alerting',
        icon: IconFont.Bell,
        label: 'Alerts',
        link: `${orgPrefix}/alerting`,
        menu: [
          {
            id: 'history',
            label: 'Alert History',
            link: `${orgPrefix}/alert-history`,
          },
        ],
      },
    ]

    return (
      <TreeNav
        expanded={true}
        headerElement={<NavHeader link={orgPrefix} />}
        userElement={<UserWidget />}
      >
        {navItems.map(item => {
          let navItemElement = (
            <TreeNav.Item
              key={item.id}
              id={item.id}
              icon={<Icon glyph={item.icon} />}
              label={item.label}
              active={getNavItemActivation([item.id], location.pathname)}
              linkElement={className => (
                <Link className={className} to={item.link} />
              )}
            >
              {!!item.menu ? (
                <TreeNav.SubMenu>
                  {item.menu.map(menuItem => {
                    let navSubItemElement = (
                      <TreeNav.SubItem
                        key={menuItem.id}
                        id={menuItem.id}
                        active={getNavItemActivation(
                          [menuItem.id],
                          location.pathname
                        )}
                        label={menuItem.label}
                        linkElement={className => (
                          <Link className={className} to={menuItem.link} />
                        )}
                      />
                    )

                    if (menuItem.cloudExclude) {
                      navSubItemElement = (
                        <CloudExclude key={menuItem.id}>
                          {navSubItemElement}
                        </CloudExclude>
                      )
                    }

                    if (menuItem.cloudOnly) {
                      navSubItemElement = (
                        <CloudOnly key={menuItem.id}>
                          {navSubItemElement}
                        </CloudOnly>
                      )
                    }

                    if (menuItem.featureFlag) {
                      navSubItemElement = (
                        <FeatureFlag
                          key={menuItem.id}
                          name={menuItem.featureFlag}
                        >
                          {navSubItemElement}
                        </FeatureFlag>
                      )
                    }

                    return navSubItemElement
                  })}
                </TreeNav.SubMenu>
              ) : null}
            </TreeNav.Item>
          )

          if (item.cloudExclude) {
            navItemElement = (
              <CloudExclude key={item.id}>{navItemElement}</CloudExclude>
            )
          }

          if (item.cloudOnly) {
            navItemElement = (
              <CloudOnly key={item.id}>{navItemElement}</CloudOnly>
            )
          }

          if (item.featureFlag) {
            navItemElement = (
              <FeatureFlag key={item.id} name={item.featureFlag}>
                {navItemElement}
              </FeatureFlag>
            )
          }

          return navItemElement
        })}
      </TreeNav>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const isHidden = state.app.ephemeral.inPresentationMode

  return {isHidden}
}

export default connect<StateProps>(mstp)(withRouter(SideNav))
