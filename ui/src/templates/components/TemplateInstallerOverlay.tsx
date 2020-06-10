// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {
  Gradients,
  Heading,
  HeadingElement,
  Overlay,
  Panel,
} from '@influxdata/clockface'

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

type Props = OwnProps & WithRouterProps

class UnconnectedImportOverlay extends PureComponent<Props> {
  public static defaultProps: {isVisible: boolean} = {
    isVisible: true,
  }

  public render() {
    const {isVisible, templateName} = this.props

    const resourceCount = 0

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
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private onDismiss = () => {
    this.props.onDismissOverlay()
  }
}

export const TemplateInstallerOverlay = withRouter<OwnProps>(
  UnconnectedImportOverlay
)
