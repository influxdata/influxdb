import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import IFQLOverlay from 'src/ifql/components/IFQLOverlay'
import {OverlayContext} from 'src/shared/components/OverlayTechnology'
import {
  showOverlay as showOverlayAction,
  ShowOverlay,
} from 'src/shared/actions/overlayTechnology'

import {Service} from 'src/types'

interface Props {
  showOverlay: ShowOverlay
  service: Service
  onGetTimeSeries: () => void
}

class IFQLHeader extends PureComponent<Props> {
  public render() {
    const {onGetTimeSeries, service} = this.props

    return (
      <div className="page-header full-width">
        <div className="page-header__container">
          <div className="page-header__left">
            <h1 className="page-header__title">Time Machine</h1>
          </div>
          <div className="page-header__right">
            <a onClick={this.overlay} href="#" />
            <button
              className="btn btn-sm btn-primary"
              onClick={onGetTimeSeries}
            >
              Get Data!
            </button>
          </div>
        </div>
      </div>
    )
  }

  private overlay() {
    const {showOverlay, service} = this.props

    showOverlay(
      <OverlayContext.Consumer>
        {({onDismissOverlay}) => (
          <IFQLOverlay
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

export default connect(null, mdtp)(IFQLHeader)
