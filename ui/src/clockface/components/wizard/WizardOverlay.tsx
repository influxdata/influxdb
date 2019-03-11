// Libraries
import React, {PureComponent} from 'react'

// Components
import OverlayBody from 'src/clockface/components/overlays/OverlayBody'
import OverlayContainer from 'src/clockface/components/overlays/OverlayContainer'
import Overlay from 'src/clockface/components/overlays/Overlay'
import OverlayHeading from 'src/clockface/components/overlays/OverlayHeading'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: any
  visible: boolean
  title: string
  maxWidth?: number
  onDismiss: () => void
}

@ErrorHandling
class WizardOverlay extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    maxWidth: 1200,
  }

  public render() {
    const {visible, title, maxWidth, children, onDismiss} = this.props

    return (
      <Overlay visible={visible}>
        <OverlayContainer maxWidth={maxWidth}>
          <OverlayHeading title={title} onDismiss={onDismiss} />
          <OverlayBody>
            <div className="data-loading--overlay">{children}</div>
          </OverlayBody>
        </OverlayContainer>
      </Overlay>
    )
  }
}

export default WizardOverlay
