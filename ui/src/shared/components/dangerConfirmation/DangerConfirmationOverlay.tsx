// Libraries
import React, {PureComponent} from 'react'

import _ from 'lodash'

// Components
import {Overlay} from '@influxdata/clockface'
import DangerConfirmationForm from 'src/shared/components/dangerConfirmation/DangerConfirmationForm'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  message: string
  effectedItems: string[]
  title: string
  onClose: () => void
  confirmButtonText: string
}

interface State {
  isConfirmed: boolean
}

@ErrorHandling
class DangerConfirmationOverlay extends PureComponent<Props, State> {
  public state = {
    isConfirmed: false,
  }

  public render() {
    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={400}>
          <Overlay.Header
            title={this.overlayTitle}
            onDismiss={this.handleCloseOverlay}
          />
          <Overlay.Body>{this.overlayContents}</Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get overlayTitle() {
    const {title} = this.props

    if (this.state.isConfirmed) {
      return title
    }

    return 'Are you sure?'
  }

  private get overlayContents() {
    const {message, effectedItems, confirmButtonText} = this.props
    if (this.state.isConfirmed) {
      return this.props.children
    }

    return (
      <DangerConfirmationForm
        onConfirm={this.handleConfirm}
        message={message}
        effectedItems={effectedItems}
        confirmButtonText={confirmButtonText}
      />
    )
  }

  private handleCloseOverlay = () => {
    this.props.onClose()
  }

  private handleConfirm = () => {
    this.setState({isConfirmed: true})
  }
}

export default DangerConfirmationOverlay
