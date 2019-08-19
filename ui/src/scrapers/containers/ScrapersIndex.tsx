// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from 'src/pageLayout'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'
import Scrapers from 'src/scrapers/components/Scrapers'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {AppState, Organization} from 'src/types'

interface StateProps {
  org: Organization
}

@ErrorHandling
class ScrapersIndex extends Component<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="scrapers" orgID={org.id}>
            <GetResources resource={ResourceTypes.Scrapers}>
              <GetResources resource={ResourceTypes.Buckets}>
                <Scrapers orgName={org.name} />
              </GetResources>
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
)(ScrapersIndex)
