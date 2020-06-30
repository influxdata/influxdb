// Libraries
import React, {Component, ComponentClass} from 'react'
import {withRouter, WithRouterProps, InjectedRouter} from 'react-router-dom'
import {connect} from 'react-redux'
import {OverlayID} from 'src/overlays/reducers/overlays'

// Actions
import {showOverlay, dismissOverlay} from 'src/overlays/actions/overlays'

// NOTE: i dont know what is wrong with the type definition of react-router
// but it doesn't include params on an injected router upon route resolution
interface RealInjectedRouter extends InjectedRouter {
  params: WithRouterProps['params']
}

export type OverlayDismissalWithRoute = (router: RealInjectedRouter) => void

interface OwnProps {
  overlayID: OverlayID
  onClose: OverlayDismissalWithRoute
}

interface DispatchProps {
  onShowOverlay: typeof showOverlay
  onDismissOverlay: typeof dismissOverlay
}

type OverlayHandlerProps = OwnProps & DispatchProps & WithRouterProps

class OverlayHandler extends Component<OverlayHandlerProps> {
  public componentWillUnmount() {
    this.props.onDismissOverlay()
  }

  public render() {
    const closer = () => {
      this.props.onClose(this.props.router as RealInjectedRouter)
    }
    const {overlayID, params, onShowOverlay} = this.props
    onShowOverlay(overlayID, params, closer)
    return null
  }
}

const mdtp: DispatchProps = {
  onShowOverlay: showOverlay,
  onDismissOverlay: dismissOverlay,
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
  overlayID: string,
  onClose?: OverlayDismissalWithRoute
): ComponentClass<P> {
  return class extends Component<P & RouteOverlayProps> {
    public render() {
      return (
        <WrappedComponent
          {...this.props}
          onClose={onClose}
          overlayID={overlayID}
        />
      )
    }
  }
}
