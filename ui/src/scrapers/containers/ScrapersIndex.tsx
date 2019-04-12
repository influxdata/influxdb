// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from 'src/pageLayout'
import {Tabs} from 'src/clockface'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import SettingsNavigation from 'src/settings/components/SettingsNavigation'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'
import Scrapers from 'src/scrapers/components/Scrapers'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Organization} from '@influxdata/influx'
import {AppState} from 'src/types'

interface StateProps {
  org: Organization
}

@ErrorHandling
class ScrapersIndex extends Component<StateProps> {
  public render() {
    const {org} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <SettingsHeader />
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <Tabs>
                <SettingsNavigation tab="scrapers" orgID={org.id} />
                <Tabs.TabContents>
                  <TabbedPageSection
                    id="org-view-tab--scrapers"
                    url="scrapers"
                    title="Scrapers"
                  >
                    <GetResources resource={ResourceTypes.Scrapers}>
                      <GetResources resource={ResourceTypes.Buckets}>
                        <Scrapers orgName={org.name} />
                        {this.props.children}
                      </GetResources>
                    </GetResources>
                  </TabbedPageSection>
                </Tabs.TabContents>
              </Tabs>
            </div>
          </Page.Contents>
        </Page>
      </>
    )
  }
}

const mstp = ({orgs: {org}}: AppState) => ({org})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(ScrapersIndex)
