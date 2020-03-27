// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import TabbedPageTabs from 'src/shared/tabbedPage/TabbedPageTabs'

// Types
import {TabbedPageTab} from 'src/shared/tabbedPage/TabbedPageTabs'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  activeTab: string
  orgID: string
}

type Props = OwnProps & WithRouterProps

@ErrorHandling
class SettingsNavigation extends PureComponent<Props> {
  public render() {
    const {activeTab, orgID, router} = this.props

    const handleTabClick = (id: string): void => {
      router.push(`/orgs/${orgID}/settings/${id}`)
    }

    const tabs: TabbedPageTab[] = [
      {
        text: 'Members',
        id: 'members',
        featureFlagName: 'treeNav',
        featureFlagValue: false,
        cloudExclude: true,
      },
      {
        text: 'Variables',
        id: 'variables',
      },
      {
        text: 'Templates',
        id: 'templates',
      },
      {
        text: 'Labels',
        id: 'labels',
      },
      {
        text: 'Profile',
        id: 'about',
        featureFlagName: 'treeNav',
        featureFlagValue: false,
      },
    ]

    return (
      <TabbedPageTabs
        tabs={tabs}
        activeTab={activeTab}
        onTabClick={handleTabClick}
      />
    )
  }
}

export default withRouter(SettingsNavigation)
