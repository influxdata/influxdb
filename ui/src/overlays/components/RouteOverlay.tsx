// Libraries
import React, {FC, Component, ComponentClass, useEffect} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'
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

type ReduxProps = ConnectedProps<typeof connector>
type OverlayHandlerProps = OwnProps & ReduxProps & RouteComponentProps

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

const mdtp = {
  onShowOverlay: showOverlay,
  onDismissOverlay: dismissOverlay,
}

const connector = connect(null, mdtp)

export default connector(withRouter(OverlayHandler))

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
