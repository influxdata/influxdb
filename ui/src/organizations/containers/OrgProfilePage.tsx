// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsNavigation from 'src/settings/components/SettingsNavigation'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import {Grid, Columns} from '@influxdata/clockface'

// Types
import {AppState, Organization} from 'src/types'
import OrgProfileTab from '../components/OrgProfileTab'

interface StateProps {
  org: Organization
}

@ErrorHandling
class OrgProfilePage extends Component<StateProps> {
  public render() {
    const {org, children} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <SettingsHeader />
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <Tabs>
                <SettingsNavigation tab="profile" orgID={org.id} />
                <Tabs.TabContents>
                  <TabbedPageSection
                    id="settings-tab--profile"
                    url="profile"
                    title="Org Profile"
                  >
                    <Grid>
                      <Grid.Row>
                        <Grid.Column
                          widthXS={Columns.Twelve}
                          widthSM={Columns.Six}
                        >
                          <OrgProfileTab />
                        </Grid.Column>
                      </Grid.Row>
                    </Grid>
                  </TabbedPageSection>
                </Tabs.TabContents>
              </Tabs>
            </div>
          </Page.Contents>
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
)(OrgProfilePage)
