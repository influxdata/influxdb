import React, {Component} from 'react'
import {
  withRouter,
  matchPath,
  RouteComponentProps,
  Switch,
  Route,
} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'
import {notify} from 'src/shared/actions/notifications'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {CommunityTemplateImportOverlay} from 'src/templates/components/CommunityTemplateImportOverlay'
import {CommunityTemplatesInstalledList} from 'src/templates/components/CommunityTemplatesInstalledList'

import {
  Bullet,
  Button,
  ComponentColor,
  ComponentSize,
  Heading,
  HeadingElement,
  Input,
  Page,
  Panel,
  FlexDirection,
  IconFont,
} from '@influxdata/clockface'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'

import {communityTemplatesImportPath} from 'src/templates/containers/TemplatesIndex'

import GetResources from 'src/resources/components/GetResources'
import {getOrg} from 'src/organizations/selectors'

import {setStagedTemplateUrl} from 'src/templates/actions/creators'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {
  getGithubUrlFromTemplateDetails,
  getTemplateNameFromUrl,
} from 'src/templates/utils'
import {reportError} from 'src/shared/utils/errors'

import {communityTemplateUnsupportedFormatError} from 'src/shared/copy/notifications'

// Types
import {AppState, ResourceType} from 'src/types'

import {event} from 'src/cloud/utils/reporting'

const communityTemplatesUrl =
  'https://github.com/influxdata/community-templates#templates'
const templatesPath = '/orgs/:orgID/settings/templates'

type Params = {
  params: {directory: string; templateName: string; templateExtension: string}
}
type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & RouteComponentProps<{templateName: string}>

@ErrorHandling
class UnconnectedTemplatesIndex extends Component<Props> {
  state = {
    templateUrl: '',
  }

  public componentDidMount() {
    // if this component mounts, and the install template is on the screen
    // (i.e. the user reloaded the page with the install template active)
    // grab the template name, and fill in the text input
    const match = matchPath(this.props.location.pathname, {
      path: communityTemplatesImportPath,
    }) as Params

    if (
      match?.params?.directory &&
      match?.params?.templateName &&
      match?.params?.templateExtension
    ) {
      this.setState({
        templateUrl: getGithubUrlFromTemplateDetails(
          match.params.directory,
          match.params.templateName,
          match.params.templateExtension
        ),
      })
    }
  }

  public render() {
    const {org} = this.props
    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Templates', 'Settings'])}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="templates" orgID={org.id}>
            {/* todo: maybe make this not a div */}
            <div className="community-templates-upload">
              <Panel className="community-templates-panel">
                <Panel.SymbolHeader
                  symbol={<Bullet text={1} size={ComponentSize.Medium} />}
                  title={
                    <Heading element={HeadingElement.H4}>
                      Find a template then return here to install it
                    </Heading>
                  }
                  size={ComponentSize.Small}
                >
                  <Button
                    color={ComponentColor.Primary}
                    size={ComponentSize.Large}
                    onClick={this.onClickBrowseCommunityTemplates}
                    text="Browse Community Templates"
                    testID="browse-template-button"
                    icon={IconFont.GitHub}
                  />
                </Panel.SymbolHeader>
              </Panel>
              <Panel className="community-templates-panel">
                <Panel.SymbolHeader
                  symbol={<Bullet text={2} size={ComponentSize.Medium} />}
                  title={
                    <Heading element={HeadingElement.H4}>
                      Paste the Template's Github URL below
                    </Heading>
                  }
                  size={ComponentSize.Small}
                />
                <Panel.Body
                  size={ComponentSize.Large}
                  direction={FlexDirection.Row}
                >
                  <Input
                    className="community-templates-template-url"
                    onChange={this.handleTemplateChange}
                    placeholder="Enter the URL of an InfluxDB Template..."
                    style={{flex: '1 0 0'}}
                    value={this.state.templateUrl}
                    testID="lookup-template-input"
                    size={ComponentSize.Large}
                  />
                  <Button
                    onClick={this.startTemplateInstall}
                    size={ComponentSize.Large}
                    text="Lookup Template"
                    testID="lookup-template-button"
                  />
                </Panel.Body>
              </Panel>
              <GetResources
                resources={[
                  ResourceType.Buckets,
                  ResourceType.Checks,
                  ResourceType.Dashboards,
                  ResourceType.Labels,
                  ResourceType.NotificationEndpoints,
                  ResourceType.NotificationRules,
                  ResourceType.Tasks,
                  ResourceType.Telegrafs,
                  ResourceType.Variables,
                ]}
              >
                <CommunityTemplatesInstalledList orgID={org.id} />
              </GetResources>
            </div>
          </SettingsTabbedPage>
        </Page>
        <Switch>
          <Route
            path={`${templatesPath}/import`}
            component={CommunityTemplateImportOverlay}
          />
        </Switch>
      </>
    )
  }

  private startTemplateInstall = () => {
    if (!this.state.templateUrl) {
      this.props.notify(communityTemplateUnsupportedFormatError())
      return false
    }

    try {
      this.props.setStagedTemplateUrl(this.state.templateUrl)

      event('template_click_lookup', {
        templateName: getTemplateNameFromUrl(this.state.templateUrl).name,
      })

      this.props.history.push(
        `/orgs/${this.props.org.id}/settings/templates/import`
      )
    } catch (err) {
      this.props.notify(communityTemplateUnsupportedFormatError())
      reportError(err, {
        name: 'The community template getTemplateDetails failed',
      })
    }
  }

  private handleTemplateChange = evt => {
    this.setState({templateUrl: evt.target.value})
  }

  private onClickBrowseCommunityTemplates = () => {
    event('template_click_browse')

    window.open(communityTemplatesUrl)
  }
}

const mstp = (state: AppState) => {
  return {
    org: getOrg(state),
  }
}

const mdtp = {
  notify,
  setStagedTemplateUrl,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplatesIndex = connector(
  withRouter(UnconnectedTemplatesIndex)
)
