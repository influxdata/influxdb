// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {AppState} from 'src/types'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import OrganizationNavigation from 'src/organizations/components/OrganizationNavigation'
import OrgHeader from 'src/organizations/containers/OrgHeader'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import Collectors from 'src/telegrafs/components/Collectors'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import GetResources, {
  ResourceTypes,
} from 'src/configuration/components/GetResources'

// Types
import {Organization} from '@influxdata/influx'

interface StateProps {
  org: Organization
}

@ErrorHandling
class TelegrafsPage extends PureComponent<StateProps> {
  public render() {
    const {org} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <OrgHeader />
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <Tabs>
                <OrganizationNavigation tab="telegrafs" orgID={org.id} />
                <Tabs.TabContents>
                  <TabbedPageSection
                    id="org-view-tab--telegrafs"
                    url="telegrafs"
                    title="Telegraf"
                  >
                    <GetResources resource={ResourceTypes.Buckets}>
                      <GetResources resource={ResourceTypes.Telegrafs}>
                        <Collectors />
                      </GetResources>
                    </GetResources>
                  </TabbedPageSection>
                </Tabs.TabContents>
              </Tabs>
            </div>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
  }
}

const mstp = ({orgs: {org}}: AppState): StateProps => ({
  org,
})

export default connect<StateProps>(mstp)(TelegrafsPage)
