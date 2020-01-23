// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import GetResources from 'src/resources/components/GetResources'
import TokensTab from 'src/authorizations/components/TokensTab'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

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
            <GetResources resources={[ResourceType.Authorizations]}>
              <TokensTab />
            </GetResources>
          </LoadDataTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = (state: AppState) => ({org: getOrg(state)})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(TokensIndex)
