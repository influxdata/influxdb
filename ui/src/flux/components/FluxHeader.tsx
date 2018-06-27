import React, {PureComponent} from 'react'

import FluxOverlay from 'src/flux/components/FluxOverlay'
import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import PageHeader from 'src/shared/components/PageHeader'

import {Service} from 'src/types'

interface Props {
  service: Service
}

interface State {
  showOverlay: boolean
}

class FluxHeader extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      showOverlay: false,
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
        {this.renderOverlay}
      </>
    )
  }

  private handleToggleOverlay = (): void => {
    this.setState({showOverlay: !this.state.showOverlay})
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

  private get renderOverlay(): JSX.Element {
    const {service} = this.props
    const {showOverlay} = this.state

    return (
      <OverlayTechnology visible={showOverlay}>
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
