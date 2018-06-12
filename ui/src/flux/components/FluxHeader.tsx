import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import FluxOverlay from 'src/flux/components/FluxOverlay'
import {OverlayContext} from 'src/shared/components/OverlayTechnology'
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
      <div className="page-header full-width">
        <div className="page-header__container">
          <div className="page-header__left">
            <h1 className="page-header__title">Time Machine</h1>
          </div>
          <div className="page-header__right">
            <button onClick={this.overlay} className="btn btn-sm btn-default">
              Edit Connection
            </button>
          </div>
        </div>
      </div>
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
