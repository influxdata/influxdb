import React, {Component} from 'react'
import {RouteComponentProps} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'
import {Switch, Route} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Page} from '@influxdata/clockface'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import TemplatesPage from 'src/templates/components/TemplatesPage'
import GetResources from 'src/resources/components/GetResources'
import TemplateImportOverlay from 'src/templates/components/TemplateImportOverlay'
import TemplateExportOverlay from 'src/templates/components/TemplateExportOverlay'
import TemplateViewOverlay from 'src/templates/components/TemplateViewOverlay'
import StaticTemplateViewOverlay from 'src/templates/components/StaticTemplateViewOverlay'

import {CommunityTemplatesIndex} from 'src/templates/containers/CommunityTemplatesIndex'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {getOrg} from 'src/organizations/selectors'

// Types
import {AppState, ResourceType} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = RouteComponentProps & ReduxProps

const templatesPath = '/orgs/:orgID/settings/templates'
export const communityTemplatesImportPath = `${templatesPath}/import/:directory/:templateName/:templateExtension`

@ErrorHandling
class TemplatesIndex extends Component<Props> {
  public render() {
    const {org, flags} = this.props
    if (flags.communityTemplates) {
      return <CommunityTemplatesIndex />
    }

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
        <Switch>
          <Route
            path={`${templatesPath}/import`}
            component={TemplateImportOverlay}
          />
          <Route
            path={`${templatesPath}/:id/export`}
            component={TemplateExportOverlay}
          />
          <Route
            path={`${templatesPath}/:id/view`}
            component={TemplateViewOverlay}
          />
          <Route
            path={`${templatesPath}/:id/static/view`}
            component={StaticTemplateViewOverlay}
          />
        </Switch>
      </>
    )
  }

  private handleImport = () => {
    const {history, org} = this.props
    history.push(`/orgs/${org.id}/settings/templates/import`)
  }
}

const mstp = (state: AppState) => {
  return {
    org: getOrg(state),
    flags: state.flags.original,
  }
}

const connector = connect(mstp)

export default connector(TemplatesIndex)
