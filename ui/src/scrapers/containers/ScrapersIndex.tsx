// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from '@influxdata/clockface'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import GetResources from 'src/resources/components/GetResources'
import Scrapers from 'src/scrapers/components/Scrapers'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

interface StateProps {
  org: Organization
}

@ErrorHandling
class ScrapersIndex extends Component<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Scrapers', 'Load Data'])}>
          <LoadDataHeader />
          <LoadDataTabbedPage activeTab="scrapers" orgID={org.id}>
            <GetResources
              resources={[ResourceType.Scrapers, ResourceType.Buckets]}
            >
              <Scrapers />
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
)(ScrapersIndex)
