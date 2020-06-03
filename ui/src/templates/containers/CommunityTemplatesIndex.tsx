import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Page} from '@influxdata/clockface'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import TemplatesPage from 'src/templates/components/TemplatesPage'
import GetResources from 'src/resources/components/GetResources'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

interface StateProps {
  org: Organization
}

type Props = WithRouterProps & StateProps

@ErrorHandling
class CTI extends Component<Props> {
  componentDidMount() {
    // eslint-disable-next-line no-console
    console.log('CommunityTemplatesIndex - the flag is working')
  }

  public render() {
    const {org, children} = this.props
    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Templates', 'Settings'])}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="templates" orgID={org.id}>
            <GetResources resources={[ResourceType.Templates]}>
              <TemplatesPage onImport={this.handleImport} />
            </GetResources>
          </SettingsTabbedPage>
        </Page>
        {children}
      </>
    )
  }

  private handleImport = () => {
    const {router, org} = this.props
    router.push(`/orgs/${org.id}/settings/templates/import`)
  }
}

const mstp = (state: AppState): StateProps => {
  return {
    org: getOrg(state),
  }
}

export const CommunityTemplatesIndex = connect<StateProps, {}, {}>(
  mstp,
  null
)(withRouter<{}>(CTI))
