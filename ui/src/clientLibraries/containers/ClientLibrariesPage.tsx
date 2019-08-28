// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import {Page} from 'src/pageLayout'
import ClientLibraries from 'src/clientLibraries/components/ClientLibraries'

// Types
import {AppState, Organization} from 'src/types'

// Mocks
import {mockClientLibraries} from 'src/clientLibraries/constants/mocks'

interface StateProps {
  org: Organization
}

@ErrorHandling
class ClientLibrariesPage extends PureComponent<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <LoadDataHeader />
          <LoadDataTabbedPage activeTab="client-libraries" orgID={org.id}>
            {/* TODO: use GetResources here when this resource exists */}
            <ClientLibraries libraries={mockClientLibraries} />
          </LoadDataTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = ({orgs: {org}}: AppState): StateProps => ({
  org,
})

export default connect<StateProps>(mstp)(ClientLibrariesPage)
