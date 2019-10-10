// Libraries
import React, {Component, ComponentClass} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Actions
import {showOverlay} from 'src/overlays/actions/overlays'

interface OwnProps {
  overlayID: string
}

interface DispatchProps {
  onShowOverlay: typeof showOverlay
}

type OverlayHandlerProps = OwnProps & DispatchProps & WithRouterProps

class OverlayHandler extends Component<OverlayHandlerProps> {
  public render() {
    const {overlayID, params, onShowOverlay} = this.props
    onShowOverlay(overlayID, params)
    return null
  }
}

const mdtp: DispatchProps = {
  onShowOverlay: showOverlay,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(withRouter<OwnProps>(OverlayHandler))

interface RouteOverlayProps {
  overlayID: string
}

export function RouteOverlay<P>(
  WrappedComponent: ComponentClass<P & RouteOverlayProps>,
  overlayID: string
): ComponentClass<P> {
  return class extends Component<P & RouteOverlayProps> {
    public render() {
      return <WrappedComponent {...this.props} overlayID={overlayID} />
    }
  }
}
