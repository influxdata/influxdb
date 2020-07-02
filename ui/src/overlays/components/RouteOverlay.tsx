// Libraries
import React, {FC, Component, ComponentClass, useEffect} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'
import {OverlayID} from 'src/overlays/reducers/overlays'

// Actions
import {showOverlay, dismissOverlay} from 'src/overlays/actions/overlays'

// NOTE(alex b): I don't know what is wrong with the type definition of react-router
// but it doesn't include params on an injected router upon route resolution

export type OverlayDismissalWithRoute = (
  history: RouteComponentProps['history'],
  params: {[x: string]: string}
) => void

interface OwnProps {
  overlayID: OverlayID
  onClose: OverlayDismissalWithRoute
}

interface DispatchProps {
  onShowOverlay: typeof showOverlay
  onDismissOverlay: typeof dismissOverlay
}

type OverlayHandlerProps = OwnProps & DispatchProps & RouteComponentProps

const OverlayHandler: FC<OverlayHandlerProps> = props => {
  const {
    overlayID,
    onShowOverlay,
    onClose,
    match,
    history,
    onDismissOverlay,
  } = props

  useEffect(() => {
    const closer = () => {
      onClose(history, match.params)
    }

    onShowOverlay(overlayID, match.params, closer)

    return () => onDismissOverlay()
  }, [overlayID])

  return null
}

const mdtp: DispatchProps = {
  onShowOverlay: showOverlay,
  onDismissOverlay: dismissOverlay,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(withRouter(OverlayHandler))

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
