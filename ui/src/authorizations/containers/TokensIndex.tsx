// Libraries
import React, {Component} from 'react'
import {Switch, Route} from 'react-router-dom'

// Components
import {Page} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import GetResources from 'src/resources/components/GetResources'
import TokensTab from 'src/authorizations/components/TokensTab'
import {
  AllAccessTokenOverlay,
  BucketsTokenOverlay,
} from 'src/overlays/components'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {ResourceType} from 'src/types'

import {ORGS, ORG_ID, TOKENS} from 'src/shared/constants/routes'

const tokensPath = `/${ORGS}/${ORG_ID}/load-data/${TOKENS}/generate`

@ErrorHandling
class TokensIndex extends Component<{}> {
  public render() {
    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Tokens', 'Load Data'])}>
          <LoadDataHeader />
          <LoadDataTabbedPage activeTab="tokens">
            <GetResources resources={[ResourceType.Authorizations]}>
              <TokensTab />
            </GetResources>
          </LoadDataTabbedPage>
        </Page>
        <Switch>
          <Route
            path={`${tokensPath}/all-access`}
            component={AllAccessTokenOverlay}
          />
          <Route
            path={`${tokensPath}/buckets`}
            component={BucketsTokenOverlay}
          />
        </Switch>
      </>
    )
  }
}

export default TokensIndex
