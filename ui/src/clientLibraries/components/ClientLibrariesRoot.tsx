// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
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
class ClientLibrariesRoot extends PureComponent<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Client Libraries', 'Load Data'])}>
          <Page.Header fullWidth={false}>
            <Page.Title title="Client Libraries" />
          </Page.Header>
          <Page.Contents fullWidth={false}>
            <ClientLibraries orgID={org.id} />
          </Page.Contents>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = (state: AppState) => ({
  org: getOrg(state),
})

export default connect<StateProps>(mstp)(ClientLibrariesRoot)
