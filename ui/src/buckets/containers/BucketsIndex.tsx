// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {Switch, Route} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import BucketsTab from 'src/buckets/components/BucketsTab'
import GetResources from 'src/resources/components/GetResources'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import LimitChecker from 'src/cloud/components/LimitChecker'
import LineProtocolWizard from 'src/buckets/components/lineProtocol/LineProtocolWizard'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import UpdateBucketOverlay from 'src/buckets/components/UpdateBucketOverlay'
import RenameBucketOverlay from 'src/buckets/components/RenameBucketOverlay'
import CreateScraperOverlay from 'src/scrapers/components/CreateScraperOverlay'
import DeleteDataOverlay from 'src/shared/components/DeleteDataOverlay'
import {Page} from '@influxdata/clockface'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

// Constants
import {ORGS, ORG_ID, BUCKETS, BUCKET_ID} from 'src/shared/constants/routes'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

interface StateProps {
  org: Organization
}

const bucketsPath = `/${ORGS}/${ORG_ID}/load-data/${BUCKETS}/${BUCKET_ID}`

@ErrorHandling
class BucketsIndex extends Component<StateProps> {
  public render() {
    const {org} = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Buckets', 'Load Data'])}>
          <LimitChecker>
            <LoadDataHeader />
            <LoadDataTabbedPage activeTab="buckets" orgID={org.id}>
              <GetResources
                resources={[
                  ResourceType.Buckets,
                  ResourceType.Labels,
                  ResourceType.Telegrafs,
                ]}
              >
                <GetAssetLimits>
                  <BucketsTab />
                </GetAssetLimits>
              </GetResources>
            </LoadDataTabbedPage>
          </LimitChecker>
        </Page>
        <Switch>
          <Route
            path={`${bucketsPath}/line-protocols/new`}
            component={LineProtocolWizard}
          />
          <Route
            path={`${bucketsPath}/telegrafs/new`}
            component={CollectorsWizard}
          />
          <Route
            path={`${bucketsPath}/scrapers/new`}
            component={CreateScraperOverlay}
          />
          <Route path={`${bucketsPath}/edit`} component={UpdateBucketOverlay} />
          <Route
            path={`${bucketsPath}/delete-data`}
            component={DeleteDataOverlay}
          />
          <Route
            path={`${bucketsPath}/rename`}
            component={RenameBucketOverlay}
          />
        </Switch>
      </>
    )
  }
}

const mstp = (state: AppState) => {
  const org = getOrg(state)

  return {org}
}

export default connect<StateProps, {}, {}>(mstp, null)(BucketsIndex)
