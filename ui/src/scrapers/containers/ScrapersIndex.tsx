// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {Switch, Route} from 'react-router-dom'

// Components
import {Page} from '@influxdata/clockface'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import GetResources from 'src/resources/components/GetResources'
import Scrapers from 'src/scrapers/components/Scrapers'
import CreateScraperOverlay from 'src/scrapers/components/CreateScraperOverlay'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

// Constants
import {ORGS, ORG_ID, SCRAPERS} from 'src/shared/constants/routes'

interface StateProps {
  org: Organization
}

const scrapersPath = `/${ORGS}/${ORG_ID}/load-data/${SCRAPERS}`

@ErrorHandling
class ScrapersIndex extends Component<StateProps> {
  public render() {
    const {org} = this.props

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
        <Switch>
          <Route
            path={`${scrapersPath}/new`}
            component={CreateScraperOverlay}
          />
        </Switch>
      </>
    )
  }
}

const mstp = (state: AppState) => ({org: getOrg(state)})

export default connect<StateProps>(mstp, null)(ScrapersIndex)
