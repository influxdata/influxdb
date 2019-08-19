// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Page} from 'src/pageLayout'
import VariablesTab from 'src/variables/components/VariablesTab'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

// Types
import {AppState, Organization} from 'src/types'

interface StateProps {
  org: Organization
}

@ErrorHandling
class VariablesIndex extends Component<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="variables" orgID={org.id}>
            <GetResources resource={ResourceTypes.Variables}>
              <VariablesTab />
            </GetResources>
          </SettingsTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = ({orgs: {org}}: AppState): StateProps => ({org})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(VariablesIndex)
