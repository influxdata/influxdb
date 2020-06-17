// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import OrgTabbedPage from 'src/organizations/components/OrgTabbedPage'
import OrgHeader from 'src/organizations/components/OrgHeader'
import {Grid, Page} from '@influxdata/clockface'

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
        <Page titleTag={pageTitleSuffixer(['About', 'Organization'])}>
          <OrgHeader />
          <OrgTabbedPage activeTab="about" orgID={org.id}>
            <Grid>
              <Grid.Row>
                <OrgProfileTab />
              </Grid.Row>
            </Grid>
          </OrgTabbedPage>
        </Page>
        {children}
      </>
    )
  }
}

const mstp = (state: AppState) => ({org: getOrg(state)})

export default connect<StateProps, {}, {}>(mstp, null)(OrgProfilePage)
