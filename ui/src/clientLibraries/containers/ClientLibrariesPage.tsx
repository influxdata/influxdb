// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import {Page} from '@influxdata/clockface'
import ClientLibraries from 'src/clientLibraries/components/ClientLibraries'

// Types
import {AppState, Organization} from 'src/types'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  org: Organization
}

@ErrorHandling
class ClientLibrariesPage extends PureComponent<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Client Libraries', 'Load Data'])}>
          <LoadDataHeader />
          <LoadDataTabbedPage activeTab="client-libraries" orgID={org.id}>
            <ClientLibraries orgID={org.id} />
          </LoadDataTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = (state: AppState): StateProps => ({
  org: getOrg(state),
})

export default connect<StateProps>(mstp)(ClientLibrariesPage)
