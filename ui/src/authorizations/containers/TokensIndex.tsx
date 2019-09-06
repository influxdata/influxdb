// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'
import TokensTab from 'src/authorizations/components/TokensTab'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

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
        <Page titleTag={pageTitleSuffixer(['Tokens', 'Load Data'])}>
          <LoadDataHeader />
          <LoadDataTabbedPage activeTab="tokens" orgID={org.id}>
            <GetResources resource={ResourceType.Authorizations}>
              <TokensTab />
            </GetResources>
          </LoadDataTabbedPage>
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
