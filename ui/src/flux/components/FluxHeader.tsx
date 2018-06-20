import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import FluxOverlay from 'src/flux/components/FluxOverlay'
import {OverlayContext} from 'src/shared/components/OverlayTechnology'
import PageHeader from 'src/shared/components/PageHeader'
import {
  showOverlay as showOverlayAction,
  ShowOverlay,
} from 'src/shared/actions/overlayTechnology'

import {Service} from 'src/types'

interface Props {
  showOverlay: ShowOverlay
  service: Service
}

class FluxHeader extends PureComponent<Props> {
  public render() {
    return (
      <PageHeader
        title="Flux Editor"
        fullWidth={true}
        renderPageControls={this.renderPageControls}
      />
    )
  }

  private renderPageControls = (): JSX.Element => {
    return (
      <button onClick={this.overlay} className="btn btn-sm btn-default">
        Edit Connection
      </button>
    )
  }

  private overlay = () => {
    const {showOverlay, service} = this.props

    showOverlay(
      <OverlayContext.Consumer>
        {({onDismissOverlay}) => (
          <FluxOverlay
            mode="edit"
            service={service}
            onDismiss={onDismissOverlay}
          />
        )}
      </OverlayContext.Consumer>,
      {}
    )
  }
}

const mdtp = {
  showOverlay: showOverlayAction,
}

export default connect(null, mdtp)(FluxHeader)
