// Libraries
import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {Alignment, Orientation, Overlay, Tabs} from '@influxdata/clockface'
import {CommunityTemplateInstallInstructions} from 'src/templates/components/CommunityTemplateInstallInstructions'
import {CommunityTemplateReadme} from 'src/templates/components/CommunityTemplateReadme'
import {CommunityTemplateResourceContent} from 'src/templates/components/CommunityTemplateResourceContent'

// Types
import {ComponentStatus} from '@influxdata/clockface'

interface OwnProps {
  isVisible?: boolean
  onDismissOverlay: () => void
  onInstall: () => void
  resourceCount: number
  status?: ComponentStatus
  templateName: string
  templateDirectory: string
  updateStatus?: (status: ComponentStatus) => void
}

interface State {
  activeTab: ActiveTab
}

enum Tab {
  IncludedResources,
  Readme,
}

type ActiveTab = Tab.IncludedResources | Tab.Readme

type Props = OwnProps & RouteComponentProps<{orgID: string}>

class CommunityTemplateOverlayUnconnected extends PureComponent<Props, State> {
  state: State = {
    activeTab: Tab.IncludedResources,
  }

  public static defaultProps: {isVisible: boolean} = {
    isVisible: true,
  }

  public render() {
    const {
      isVisible,
      onInstall,
      resourceCount,
      templateName,
      templateDirectory,
    } = this.props

    return (
      <Overlay visible={isVisible}>
        <Overlay.Container maxWidth={800} testID="template-install-overlay">
          <Overlay.Header
            title="Template Installer"
            onDismiss={this.onDismiss}
          />
          <Overlay.Body>
            <CommunityTemplateInstallInstructions
              templateName={templateName}
              resourceCount={resourceCount}
              onClickInstall={onInstall}
            />
            <Tabs.Container orientation={Orientation.Horizontal}>
              <Tabs.Tabs alignment={Alignment.Center}>
                <Tabs.Tab
                  active={this.state.activeTab === Tab.IncludedResources}
                  id="included-resources"
                  text="Included Resources"
                  onClick={this.setTabToIncludedResources}
                />
                <Tabs.Tab
                  active={this.state.activeTab === Tab.Readme}
                  id="readme"
                  text="Readme"
                  testID="community-templates-readme-tab"
                  onClick={this.setTabToReadme}
                />
              </Tabs.Tabs>
              {this.state.activeTab === Tab.IncludedResources ? (
                <CommunityTemplateResourceContent />
              ) : (
                <CommunityTemplateReadme directory={templateDirectory} />
              )}
            </Tabs.Container>
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private setTabToIncludedResources = () => {
    this.setState({activeTab: Tab.IncludedResources})
  }

  private setTabToReadme = () => {
    this.setState({activeTab: Tab.Readme})
  }

  private onDismiss = () => {
    this.props.onDismissOverlay()
  }
}

export const CommunityTemplateOverlay = withRouter(
  CommunityTemplateOverlayUnconnected
)
