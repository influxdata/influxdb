import React, {Component} from 'react'
import {withRouter, RouteComponentProps, matchPath} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
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
  getGithubUrlFromTemplateName,
  getTemplateNameFromGithubUrl,
} from 'src/templates/utils'

// Types
import {AppState, Organization} from 'src/types'

const communityTemplatesUrl =
  'https://github.com/influxdata/community-templates#templates'

interface StateProps {
  org: Organization
}

type Params = {params: {templateName: string}}

type Props = StateProps & RouteComponentProps<{templateName: string}>

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

    if (match?.params?.templateName) {
      this.setState({
        templateUrl: getGithubUrlFromTemplateName(match.params.templateName),
      })
    }
  }

  public render() {
    const {org, children} = this.props
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
            </div>
          </SettingsTabbedPage>
        </Page>
        {children}
      </>
    )
  }

  private startTemplateInstall = () => {
    if (!this.state.templateUrl) {
      console.error('undefined')
      return false
    }

    const name = getTemplateNameFromGithubUrl(this.state.templateUrl)
    this.showInstallerOverlay(name)
  }

  private showInstallerOverlay = templateName => {
    const {history, org} = this.props

    history.push(`/orgs/${org.id}/settings/templates/import/${templateName}`)
  }

  private handleTemplateChange = event => {
    this.setState({templateUrl: event.target.value})
  }
}

const mstp = (state: AppState): StateProps => {
  return {
    org: getOrg(state),
  }
}

export const CommunityTemplatesIndex = connect<StateProps>(mstp)(
  withRouter(UnconnectedTemplatesIndex)
)
