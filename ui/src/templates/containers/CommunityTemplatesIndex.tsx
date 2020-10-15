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
import {CommunityTemplateInstallOverlay} from 'src/templates/components/CommunityTemplateInstallOverlay'
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
  FlexBox,
  IconFont,
  ComponentStatus,
  AlignItems,
} from '@influxdata/clockface'
import SettingsTabbedPage from 'src/settings/components/SettingsTabbedPage'
import SettingsHeader from 'src/settings/components/SettingsHeader'

import GetResources from 'src/resources/components/GetResources'
import {getOrg} from 'src/organizations/selectors'

import {setStagedTemplateUrl} from 'src/templates/actions/creators'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {
  getGithubUrlFromTemplateDetails,
  getTemplateNameFromUrl,
} from 'src/templates/utils'

import {reportErrorThroughHoneyBadger} from 'src/shared/utils/errors'
import {communityTemplateUnsupportedFormatError} from 'src/shared/copy/notifications'

import {
  validateTemplateURL,
  TEMPLATE_URL_VALID,
  TEMPLATE_URL_WARN,
} from 'src/templates/utils'

// Types
import {AppState, ResourceType} from 'src/types'

import {event} from 'src/cloud/utils/reporting'

const communityTemplatesUrl =
  'https://github.com/influxdata/community-templates#templates'
const templatesPath = '/orgs/:orgID/settings/templates'
const communityTemplatesImportPath = `${templatesPath}/import/:directory/:templateName/:templateExtension`

type Params = {
  params: {directory: string; templateName: string; templateExtension: string}
}
type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & RouteComponentProps<{templateName: string}>

interface State {
  validationMessage: string
}

@ErrorHandling
class UnconnectedCommunityTemplatesIndex extends Component<Props, State> {
  state = {
    validationMessage: '',
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
      this.props.setStagedTemplateUrl(
        getGithubUrlFromTemplateDetails(
          match.params.directory,
          match.params.templateName,
          match.params.templateExtension
        )
      )
    }
  }

  public render() {
    const {org} = this.props
    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Templates', 'Settings'])}>
          <SettingsHeader />
          <SettingsTabbedPage activeTab="templates" orgID={org.id}>
            <FlexBox
              direction={FlexDirection.Column}
              margin={ComponentSize.Small}
              stretchToFitWidth={true}
              alignItems={AlignItems.Stretch}
            >
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
                      Paste the URL of the Template's resource manifest file
                    </Heading>
                  }
                  size={ComponentSize.Small}
                />
                <Panel.Body
                  size={ComponentSize.Large}
                  direction={FlexDirection.Column}
                >
                  <p>
                    Every template has a file that controls what gets installed,
                    which is usually a YAML or JSON file
                  </p>
                  <FlexBox
                    direction={FlexDirection.Row}
                    margin={ComponentSize.Large}
                    stretchToFitWidth={true}
                  >
                    <Input
                      className="community-templates-template-url"
                      onChange={this.handleTemplateChange}
                      onKeyPress={this.handleInputKeyPress}
                      placeholder="https://github.com/influxdata/community-templates/blob/master/example/example.yml"
                      style={{flex: '1 0 0'}}
                      value={this.props.stagedTemplateUrl}
                      testID="lookup-template-input"
                      size={ComponentSize.Large}
                      status={this.inputStatus}
                    />
                    <Button
                      onClick={this.startTemplateInstall}
                      size={ComponentSize.Large}
                      text="Lookup Template"
                      testID="lookup-template-button"
                      className="community-templates--browse"
                    />
                  </FlexBox>
                  {this.inputFeedback}
                </Panel.Body>
              </Panel>
            </FlexBox>

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
          </SettingsTabbedPage>
        </Page>
        <Switch>
          <Route
            path={`${templatesPath}/import`}
            render={props => {
              return (
                <CommunityTemplateInstallOverlay
                  {...props}
                  setTemplateUrlValidationMessage={this.setValidationMessage}
                />
              )
            }}
          />
        </Switch>
      </>
    )
  }

  private get inputFeedback(): JSX.Element | null {
    const feedbackClassName = `community-templates-template-url--feedback community-templates-template-url--feedback__${this.inputStatus}`

    if (this.state.validationMessage) {
      return <p className={feedbackClassName}>{this.state.validationMessage}</p>
    }
  }

  private get inputStatus(): ComponentStatus {
    if (
      this.state.validationMessage === '' ||
      this.state.validationMessage === TEMPLATE_URL_WARN
    ) {
      return ComponentStatus.Default
    } else if (this.state.validationMessage === TEMPLATE_URL_VALID) {
      return ComponentStatus.Valid
    }

    return ComponentStatus.Error
  }

  private startTemplateInstall = () => {
    if (!this.props.stagedTemplateUrl) {
      this.props.notify(communityTemplateUnsupportedFormatError())
      return false
    }

    try {
      event('template_click_lookup', {
        templateName: getTemplateNameFromUrl(this.props.stagedTemplateUrl).name,
      })

      this.props.history.push(
        `/orgs/${this.props.org.id}/settings/templates/import`
      )
    } catch (err) {
      this.props.notify(communityTemplateUnsupportedFormatError())
      reportErrorThroughHoneyBadger(err, {
        name: 'The community template getTemplateDetails failed',
      })
    }
  }

  private handleTemplateChange = event => {
    const validationMessage = validateTemplateURL(event.target.value)

    this.setValidationMessage(validationMessage)
    this.props.setStagedTemplateUrl(event.target.value)
  }

  private handleInputKeyPress = event => {
    if (event.key === 'Enter') {
      this.startTemplateInstall()
    }
  }

  private onClickBrowseCommunityTemplates = () => {
    event('template_click_browse')

    window.open(communityTemplatesUrl)
  }

  private setValidationMessage = (validationMessage: string) => {
    this.setState({validationMessage})
  }
}

const mstp = (state: AppState) => {
  return {
    org: getOrg(state),
    stagedTemplateUrl: state.resources.templates.stagedTemplateUrl,
  }
}

const mdtp = {
  notify,
  setStagedTemplateUrl,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplatesIndex = connector(
  withRouter(UnconnectedCommunityTemplatesIndex)
)
