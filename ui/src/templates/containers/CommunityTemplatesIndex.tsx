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
  LinkButton,
  LinkTarget,
  Page,
  Panel,
} from '@influxdata/clockface'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'

import {communityTemplatesImportPath} from 'src/templates/containers/TemplatesIndex'

import {getOrg} from 'src/organizations/selectors'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {
  getGithubUrlFromTemplateDetails,
  getTemplateDetails,
} from 'src/templates/utils'

import {communityTemplateUnsupportedFormatError} from 'src/shared/copy/notifications'
// Types
import {AppState} from 'src/types'

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
              <Panel className="community-templates-upload-panel">
                <Panel.SymbolHeader
                  symbol={<Bullet text={1} size={ComponentSize.Medium} />}
                  title={
                    <Heading element={HeadingElement.H4}>
                      Find a template then return here to install it
                    </Heading>
                  }
                  size={ComponentSize.Small}
                >
                  <LinkButton
                    color={ComponentColor.Primary}
                    href={communityTemplatesUrl}
                    size={ComponentSize.Small}
                    target={LinkTarget.Blank}
                    text="Browse Community Templates"
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
                  size={ComponentSize.Medium}
                />
                <Panel.Body size={ComponentSize.Large}>
                  <div>
                    <Input
                      className="community-templates-template-url"
                      onChange={this.handleTemplateChange}
                      placeholder="Enter the URL of an InfluxDB Template..."
                      style={{width: '80%'}}
                      value={this.state.templateUrl}
                    />
                    <Button
                      onClick={this.startTemplateInstall}
                      size={ComponentSize.Small}
                      text="Lookup Template"
                    />
                  </div>
                </Panel.Body>
              </Panel>
              <CommunityTemplatesInstalledList orgID={org.id} />
            </div>
          </SettingsTabbedPage>
        </Page>
        <Switch>
          <Route
            path={`${templatesPath}/import/:directory/:templateName/:templateExtension`}
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
      const {directory, templateExtension, templateName} = getTemplateDetails(
        this.state.templateUrl
      )

      this.props.history.push(
        `/orgs/${this.props.org.id}/settings/templates/import/${directory}/${templateName}/${templateExtension}`
      )
    } catch (err) {
      this.props.notify(communityTemplateUnsupportedFormatError())
    }
  }

  private handleTemplateChange = event => {
    this.setState({templateUrl: event.target.value})
  }
}

const mstp = (state: AppState) => {
  return {
    org: getOrg(state),
  }
}

const mdtp = {
  notify,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplatesIndex = connector(
  withRouter(UnconnectedTemplatesIndex)
)
