// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {withRouter, RouteComponentProps} from 'react-router-dom'

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

type Props = OwnProps & RouteComponentProps<{orgID: string}>

@ErrorHandling
class SettingsNavigation extends PureComponent<Props> {
  public render() {
    const {activeTab, orgID, history} = this.props

    const handleTabClick = (id: string): void => {
      history.push(`/orgs/${orgID}/settings/${id}`)
    }

    const tabs: TabbedPageTab[] = [
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
