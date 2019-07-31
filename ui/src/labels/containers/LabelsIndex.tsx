// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsNavigation from 'src/settings/components/SettingsNavigation'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import LabelsTab from 'src/labels/components/LabelsTab'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

// Types
import {AppState, Organization} from 'src/types'

interface StateProps {
  org: Organization
}

@ErrorHandling
class LabelsIndex extends PureComponent<StateProps> {
  public render() {
    const {org} = this.props

    return (
      <Page titleTag={org.name}>
        <SettingsHeader />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <Tabs>
              <SettingsNavigation tab="labels" orgID={org.id} />
              <Tabs.TabContents>
                <TabbedPageSection
                  id="settings-tab--labels"
                  url="labels"
                  title="Labels"
                >
                  <GetResources resource={ResourceTypes.Labels}>
                    <LabelsTab />
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
)(LabelsIndex)
