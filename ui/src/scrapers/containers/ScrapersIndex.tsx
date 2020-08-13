// Libraries
import React, {Component} from 'react'
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

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ResourceType} from 'src/types'

// Constants
import {ORGS, ORG_ID, SCRAPERS} from 'src/shared/constants/routes'

const scrapersPath = `/${ORGS}/${ORG_ID}/load-data/${SCRAPERS}`

@ErrorHandling
class ScrapersIndex extends Component<{}> {
  public render() {
    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Scrapers', 'Load Data'])}>
          <LoadDataHeader />
          <LoadDataTabbedPage activeTab="scrapers">
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

export default ScrapersIndex
