import React, {PureComponent} from 'react'

import FluxOverlay from 'src/flux/components/FluxOverlay'
import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'

import {Service} from 'src/types'

interface Props {
  service: Service
}

interface State {
  isOverlayVisible: boolean
}

class FluxHeader extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      isOverlayVisible: false,
    }
  }

  public render() {
    return (
      <>
        <PageHeader
          titleText="Flux Editor"
          fullWidth={true}
          optionsComponents={this.optionsComponents}
        />
        {this.overlay}
      </>
    )
  }

  private handleToggleOverlay = (): void => {
    this.setState({isOverlayVisible: !this.state.isOverlayVisible})
  }

  private get optionsComponents(): JSX.Element {
    return (
      <button
        onClick={this.handleToggleOverlay}
        className="btn btn-sm btn-default"
      >
        Edit Connection
      </button>
    )
  }

  private get overlay(): JSX.Element {
    const {service} = this.props
    const {isOverlayVisible} = this.state

    return (
      <OverlayTechnology visible={isOverlayVisible}>
        <FluxOverlay
          mode="edit"
          service={service}
          onDismiss={this.handleToggleOverlay}
        />
      </OverlayTechnology>
    )
  }
}

export default FluxHeader
