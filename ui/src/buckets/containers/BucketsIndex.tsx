// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import {Page} from 'src/pageLayout'
import BucketsTab from 'src/buckets/components/BucketsTab'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'

// Types
import {AppState, Organization} from 'src/types'

interface StateProps {
  org: Organization
}

@ErrorHandling
class BucketsIndex extends Component<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <LoadDataHeader />
          <LoadDataTabbedPage activeTab="buckets" orgID={org.id}>
            <GetResources resource={ResourceTypes.Buckets}>
              <GetResources resource={ResourceTypes.Telegrafs}>
                <GetAssetLimits>
                  <BucketsTab />
                </GetAssetLimits>
              </GetResources>
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
)(BucketsIndex)
