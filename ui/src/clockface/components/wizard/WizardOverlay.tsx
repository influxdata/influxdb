// Libraries
import React, {PureComponent} from 'react'

// Components
import {Overlay} from '@influxdata/clockface'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: string | React.ReactNode
  footer?: string | React.ReactNode
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
    const {title, maxWidth, children, footer, onDismiss} = this.props

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={maxWidth}>
          <Overlay.Header title={title} onDismiss={onDismiss} />
          <Overlay.Body>
            <div className="data-loading--overlay">{children}</div>
          </Overlay.Body>
          {footer && <Overlay.Footer>{footer}</Overlay.Footer>}
        </Overlay.Container>
      </Overlay>
    )
  }
}

export default WizardOverlay
