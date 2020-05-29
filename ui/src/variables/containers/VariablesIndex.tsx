// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Page} from '@influxdata/clockface'
import VariablesTab from 'src/variables/components/VariablesTab'
import GetResources from 'src/resources/components/GetResources'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

interface StateProps {
  org: Organization
}

@ErrorHandling
class VariablesIndex extends Component<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Variables', 'Settings'])}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="variables" orgID={org.id}>
            <GetResources resources={[ResourceType.Variables]}>
              <VariablesTab />
            </GetResources>
          </SettingsTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  return {org: state.resources.orgs.org}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(VariablesIndex)
