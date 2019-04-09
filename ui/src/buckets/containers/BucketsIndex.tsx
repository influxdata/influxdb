// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import OrganizationNavigation from 'src/organizations/components/OrganizationNavigation'
import OrgHeader from 'src/organizations/containers/OrgHeader'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import BucketsTab from 'src/buckets/components/BucketsTab'
import GetResources, {
  ResourceTypes,
} from 'src/configuration/components/GetResources'

// Types
import {Organization} from '@influxdata/influx'
import {AppState} from 'src/types'

interface StateProps {
  org: Organization
}

@ErrorHandling
class BucketsIndex extends Component<StateProps> {
  public render() {
    const {org} = this.props

    return (
      <Page titleTag={org.name}>
        <OrgHeader />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <Tabs>
              <OrganizationNavigation tab="buckets" orgID={org.id} />
              <Tabs.TabContents>
                <TabbedPageSection
                  id="org-view-tab--buckets"
                  url="buckets"
                  title="Buckets"
                >
                  <GetResources resource={ResourceTypes.Buckets}>
                    <BucketsTab />
                  </GetResources>
                </TabbedPageSection>
              </Tabs.TabContents>
            </Tabs>
          </div>
        </Page.Contents>
      </Page>
    )
  }
}

const mstp = ({orgs: {org}}: AppState) => ({org})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(BucketsIndex)
