import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Page} from '@influxdata/clockface'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import TemplatesPage from 'src/templates/components/TemplatesPage'
import GetResources from 'src/resources/components/GetResources'

import {CommunityTemplatesIndex} from 'src/templates/containers/CommunityTemplatesIndex'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

// Types
import {AppState, Organization, ResourceType} from 'src/types'
import {FlagMap} from 'src/shared/reducers/flags'

interface StateProps {
  flags: FlagMap
  org: Organization
}

type Props = WithRouterProps & StateProps

@ErrorHandling
class TemplatesIndex extends Component<Props> {
  public render() {
    const {org, children, flags} = this.props
    if (flags.communityTemplates) {
      return <CommunityTemplatesIndex>{children}</CommunityTemplatesIndex>
    }
    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Templates', 'Settings'])}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="templates" orgID={org.id}>
            <GetResources resources={[ResourceType.Templates]}>
              <TemplatesPage onImport={this.handleImport} />
            </GetResources>
          </SettingsTabbedPage>
        </Page>
        {children}
      </>
    )
  }

  private handleImport = () => {
    const {router, org} = this.props
    router.push(`/orgs/${org.id}/settings/templates/import`)
  }
}

const mstp = (state: AppState): StateProps => {
  return {
    org: getOrg(state),
    flags: state.flags.original,
  }
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(withRouter<{}>(TemplatesIndex))
