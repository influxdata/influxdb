// Libraries
import React, {PureComponent, ReactElement} from 'react'

// Components
import OverlayBody from 'src/clockface/components/overlays/OverlayBody'
import OverlayContainer from 'src/clockface/components/overlays/OverlayContainer'
import OverlayTechnology from 'src/clockface/components/overlays/OverlayTechnology'
import WizardController from 'src/clockface/components/wizard/WizardController'
import OverlayHeading from 'src/clockface/components/overlays/OverlayHeading'

import {WizardStepProps} from 'src/clockface/components/wizard/WizardStep'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {ToggleWizard} from 'src/types/wizard'

interface Props {
  children: Array<ReactElement<WizardStepProps>>
  visible: boolean
  title: string
  toggleVisibility: ToggleWizard
  resetWizardState: () => void
  skipLinkText?: string
  maxWidth?: number
  jumpStep: number
}

@ErrorHandling
class WizardOverlay extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    maxWidth: 800,
  }

  public render() {
    const {visible, title, maxWidth} = this.props

    return (
      <OverlayTechnology visible={visible}>
        <OverlayContainer maxWidth={maxWidth}>
          <OverlayHeading title={title} />
          <OverlayBody>{this.WizardController}</OverlayBody>
        </OverlayContainer>
      </OverlayTechnology>
    )
  }

  private get WizardController() {
    const {children, skipLinkText, jumpStep} = this.props
    if (children) {
      return (
        <WizardController
          skipLinkText={skipLinkText}
          handleSkip={this.handleSkip}
          jumpStep={jumpStep}
        >
          {children}
        </WizardController>
      )
    }

    return null
  }

  private handleSkip = () => {
    const {toggleVisibility, resetWizardState} = this.props
    toggleVisibility(false)()
    resetWizardState()
  }
}

export default WizardOverlay
