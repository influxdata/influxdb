// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {
  Alignment,
  Gradients,
  Heading,
  HeadingElement,
  Orientation,
  Overlay,
  Panel,
  Tabs,
} from '@influxdata/clockface'

import {CommunityTemplateReadme} from 'src/templates/components/CommunityTemplateReadme'
import {CommunityTemplateContents} from 'src/templates/components/CommunityTemplateContents'

// Types
import {ComponentStatus} from '@influxdata/clockface'

interface OwnProps {
  isVisible?: boolean
  onDismissOverlay: () => void
  onSubmit: (importString: string, orgID: string) => void
  status?: ComponentStatus
  templateName: string
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

type Props = OwnProps & WithRouterProps

class CommunityTemplateInstallerOverlayUnconnected extends PureComponent<
  Props,
  State
> {
  state: State = {
    activeTab: Tab.IncludedResources,
  }

  public static defaultProps: {isVisible: boolean} = {
    isVisible: true,
  }

  public render() {
    const {isVisible, templateName} = this.props

    const resourceCount = 'some'

    return (
      <Overlay visible={isVisible}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Header
            title="Template Installer"
            onDismiss={this.onDismiss}
          />
          <Overlay.Body>
            <Panel border={true} gradient={Gradients.RebelAlliance}>
              <Panel.Header>
                <Heading element={HeadingElement.H3}>{templateName}</Heading>
              </Panel.Header>
              <Panel.Body>
                Installing this template will create {resourceCount} resources
                in your system
              </Panel.Body>
            </Panel>
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
                  onClick={this.setTabToReadme}
                />
              </Tabs.Tabs>
              {this.state.activeTab === Tab.IncludedResources ? (
                <CommunityTemplateContents />
              ) : (
                <CommunityTemplateReadme />
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

export const CommunityTemplateInstallerOverlay = withRouter<OwnProps>(
  CommunityTemplateInstallerOverlayUnconnected
)
