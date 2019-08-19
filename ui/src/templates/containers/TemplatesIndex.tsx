import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Page} from 'src/pageLayout'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import TemplatesPage from 'src/templates/components/TemplatesPage'

// Types
import {AppState, Organization} from 'src/types'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

interface StateProps {
  org: Organization
}

type Props = WithRouterProps & StateProps

@ErrorHandling
class TemplatesIndex extends Component<Props> {
  public render() {
    const {org, children} = this.props
    return (
      <>
        <Page titleTag={org.name}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="templates" orgID={org.id}>
            <GetResources resource={ResourceTypes.Templates}>
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
    router.push(`/orgs/${org.id}/templates/import`)
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    orgs: {org},
  } = state

  return {
    org,
  }
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(withRouter<{}>(TemplatesIndex))
