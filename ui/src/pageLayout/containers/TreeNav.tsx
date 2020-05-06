// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps, Link} from 'react-router'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {Icon, TreeNav} from '@influxdata/clockface'
import UserWidget from 'src/pageLayout/components/UserWidget'
import NavHeader from 'src/pageLayout/components/NavHeader'
import CloudUpgradeNavBanner from 'src/shared/components/CloudUpgradeNavBanner'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'
import OrgSettings from 'src/cloud/components/OrgSettings'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Constants
import {generateNavItems} from 'src/pageLayout/constants/navigationHierarchy'
import {
  HIDE_UPGRADE_CTA_KEY,
  PAID_ORG_HIDE_UPGRADE_SETTING,
} from 'src/cloud/constants'

// Utils
import {getNavItemActivation} from 'src/pageLayout/utils'

// Types
import {AppState, NavBarState, OrgSetting} from 'src/types'

// Actions
import {setNavBarState} from 'src/shared/actions/app'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface StateProps {
  isHidden: boolean
  navBarState: NavBarState
  showUpgradeButton: boolean
}

interface DispatchProps {
  handleSetNavBarState: typeof setNavBarState
}

type Props = StateProps & DispatchProps & WithRouterProps

@ErrorHandling
class TreeSidebar extends PureComponent<Props> {
  public render() {
    const {
      isHidden,
      params: {orgID},
      navBarState,
      handleSetNavBarState,
      showUpgradeButton,
    } = this.props

    if (isHidden) {
      return null
    }

    const isExpanded = navBarState === 'expanded'

    const handleToggleNavExpansion = (): void => {
      if (isExpanded) {
        handleSetNavBarState('collapsed')
      } else {
        handleSetNavBarState('expanded')
      }
    }

    const orgPrefix = `/orgs/${orgID}`
    const navItems = generateNavItems(orgID)

    return (
      <OrgSettings>
        <TreeNav
          expanded={isExpanded}
          headerElement={<NavHeader link={orgPrefix} />}
          userElement={<UserWidget />}
          onToggleClick={handleToggleNavExpansion}
          bannerElement={showUpgradeButton ? <CloudUpgradeNavBanner /> : null}
        >
          {navItems.map(item => {
            const linkElement = (className: string): JSX.Element => {
              if (item.link.type === 'href') {
                return <a href={item.link.location} className={className} />
              }

              return <Link to={item.link.location} className={className} />
            }
            let navItemElement = (
              <TreeNav.Item
                key={item.id}
                id={item.id}
                testID={item.testID}
                icon={<Icon glyph={item.icon} />}
                label={item.label}
                shortLabel={item.shortLabel}
                active={getNavItemActivation(
                  item.activeKeywords,
                  location.pathname
                )}
                linkElement={linkElement}
              >
                {Boolean(item.menu) && (
                  <TreeNav.SubMenu>
                    {item.menu.map(menuItem => {
                      const linkElement = (className: string): JSX.Element => {
                        if (menuItem.link.type === 'href') {
                          return (
                            <a
                              href={menuItem.link.location}
                              className={className}
                            />
                          )
                        }

                        return (
                          <Link
                            to={menuItem.link.location}
                            className={className}
                          />
                        )
                      }

                      let navSubItemElement = (
                        <TreeNav.SubItem
                          key={menuItem.id}
                          id={menuItem.id}
                          testID={menuItem.testID}
                          active={getNavItemActivation(
                            [menuItem.id],
                            location.pathname
                          )}
                          label={menuItem.label}
                          linkElement={linkElement}
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
                            equals={menuItem.featureFlagValue}
                          >
                            {navSubItemElement}
                          </FeatureFlag>
                        )
                      }

                      return navSubItemElement
                    })}
                  </TreeNav.SubMenu>
                )}
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
                <FeatureFlag
                  key={item.id}
                  name={item.featureFlag}
                  equals={item.featureFlagValue}
                >
                  {navItemElement}
                </FeatureFlag>
              )
            }

            return navItemElement
          })}
        </TreeNav>
      </OrgSettings>
    )
  }
}

const mdtp: DispatchProps = {
  handleSetNavBarState: setNavBarState,
}

const mstp = (state: AppState): StateProps => {
  const isHidden = get(state, 'app.ephemeral.inPresentationMode', false)
  const navBarState = get(state, 'app.persisted.navBarState', 'collapsed')
  const {settings} = get(state, 'cloud.orgSettings')
  let showUpgradeButton = false
  const hideUpgradeCTA = settings.find(
    (setting: OrgSetting) => setting.key === HIDE_UPGRADE_CTA_KEY
  )
  if (
    !hideUpgradeCTA ||
    hideUpgradeCTA.value !== PAID_ORG_HIDE_UPGRADE_SETTING.value
  ) {
    showUpgradeButton = true
  }
  return {isHidden, navBarState, showUpgradeButton}
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter(TreeSidebar))
