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
import {generateNavItems} from 'src/pageLayout/constants/navigationHierarchy'

// Utils
import {getNavItemActivation} from 'src/pageLayout/utils'

// Types
import {AppState} from 'src/types'

// Actions
import {expandNavTree, collapseNavTree} from 'src/shared/actions/app'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface StateProps {
  isHidden: boolean
  navTreeExpanded: boolean
}

interface DispatchProps {
  handleExpandNavTree: typeof expandNavTree
  handleCollapseNavTree: typeof collapseNavTree
}

interface State {
  isShowingOrganizations: boolean
}

type Props = StateProps & DispatchProps & WithRouterProps

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
      navTreeExpanded,
      handleExpandNavTree,
      handleCollapseNavTree,
    } = this.props

    if (isHidden) {
      return null
    }

    const handleToggleNavExpansion = (): void => {
      if (navTreeExpanded) {
        handleCollapseNavTree()
      } else {
        handleExpandNavTree()
      }
    }

    const orgPrefix = `/orgs/${orgID}`
    const navItems = generateNavItems(orgID)

    return (
      <TreeNav
        expanded={navTreeExpanded}
        headerElement={<NavHeader link={orgPrefix} />}
        userElement={<UserWidget />}
        onToggleClick={handleToggleNavExpansion}
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

const mdtp: DispatchProps = {
  handleExpandNavTree: expandNavTree,
  handleCollapseNavTree: collapseNavTree,
}

const mstp = (state: AppState): StateProps => {
  const isHidden = state.app.ephemeral.inPresentationMode
  const navTreeExpanded = state.app.persisted.navTreeExpanded

  return {isHidden, navTreeExpanded}
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter(SideNav))
