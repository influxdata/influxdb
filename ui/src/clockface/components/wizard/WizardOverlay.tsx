// Libraries
import React, {PureComponent} from 'react'

// Components
import {Overlay} from 'src/clockface'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: any
  visible: boolean
  title: string
  maxWidth: number
  onDismiss: () => void
}

@ErrorHandling
class WizardOverlay extends PureComponent<Props> {
  public static defaultProps = {
    maxWidth: 1200,
  }

  public render() {
    const {visible, title, maxWidth, children, onDismiss} = this.props

    return (
      <Overlay visible={visible}>
        <Overlay.Container maxWidth={maxWidth}>
          <Overlay.Heading title={title} onDismiss={onDismiss} />
          <Overlay.Body>
            <div className="data-loading--overlay">{children}</div>
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }
}

export default WizardOverlay
