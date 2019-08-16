// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Page} from 'src/pageLayout'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'
import TokensTab from 'src/authorizations/components/TokensTab'

// Types
import {AppState, Organization} from 'src/types'

interface StateProps {
  org: Organization
}

@ErrorHandling
class TokensIndex extends Component<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="tokens" orgID={org.id}>
            <GetResources resource={ResourceTypes.Authorizations}>
              <TokensTab />
            </GetResources>
          </SettingsTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = ({orgs: {org}}: AppState) => ({org})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(TokensIndex)
