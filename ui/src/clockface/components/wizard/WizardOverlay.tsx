// Libraries
import React, {PureComponent} from 'react'

// Components
import OverlayBody from 'src/clockface/components/overlays/OverlayBody'
import OverlayContainer from 'src/clockface/components/overlays/OverlayContainer'
import OverlayTechnology from 'src/clockface/components/overlays/OverlayTechnology'
import OverlayHeading from 'src/clockface/components/overlays/OverlayHeading'

import {ErrorHandling} from 'src/shared/decorators/errors'

import {ToggleWizard} from 'src/types/wizard'

interface Props {
  children: any
  visible: boolean
  title: string
  toggleVisibility: ToggleWizard
  resetWizardState: () => void
  maxWidth?: number
  jumpStep: number
  onDismis: () => void
}

@ErrorHandling
class WizardOverlay extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    maxWidth: 1200,
  }

  public render() {
    const {visible, title, maxWidth, children, onDismis} = this.props

    return (
      <OverlayTechnology visible={visible}>
        <OverlayContainer maxWidth={maxWidth}>
          <OverlayHeading title={title} onDismiss={onDismis} />
          <OverlayBody>
            <div className="wizard-overlay">{children}</div>
          </OverlayBody>
        </OverlayContainer>
      </OverlayTechnology>
    )
  }
}

export default WizardOverlay
