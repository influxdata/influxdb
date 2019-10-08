import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Page} from '@influxdata/clockface'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import TemplatesPage from 'src/templates/components/TemplatesPage'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {AppState, Organization} from 'src/types'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'

interface StateProps {
  org: Organization
}

@ErrorHandling
class TemplatesIndex extends Component<StateProps> {
  public render() {
    const {org, children} = this.props
    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Templates', 'Settings'])}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="templates" orgID={org.id}>
            <GetResources resource={ResourceType.Templates}>
              <TemplatesPage />
            </GetResources>
          </SettingsTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    orgs: {org},
  } = state

  return {
    org,
  }
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(TemplatesIndex)
