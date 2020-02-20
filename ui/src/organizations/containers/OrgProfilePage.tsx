// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import {Grid, Columns, Page} from '@influxdata/clockface'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

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
        <Page titleTag={pageTitleSuffixer(['Org Profile', 'Settings'])}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="profile" orgID={org.id}>
            <Grid>
              <Grid.Row>
                <Grid.Column widthXS={Columns.Twelve} widthSM={Columns.Six}>
                  <OrgProfileTab />
                </Grid.Column>
              </Grid.Row>
            </Grid>
          </SettingsTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = (state: AppState) => ({org: getOrg(state)})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(OrgProfilePage)
