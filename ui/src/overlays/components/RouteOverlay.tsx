// Libraries
import React, {FC, Component, ComponentClass, useEffect} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {useDispatch} from 'react-redux'
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

type OverlayHandlerProps = OwnProps & RouteComponentProps

const OverlayHandler: FC<OverlayHandlerProps> = props => {
  const {overlayID, onClose, match, history} = props

  const dispatch = useDispatch()

  useEffect(() => {
    const closer = () => {
      onClose(history, match.params)
    }

    dispatch(showOverlay(overlayID, match.params, closer))

    return () => dispatch(dismissOverlay())
  }, [overlayID]) // eslint-disable-line react-hooks/exhaustive-deps

  return null
}

const routedComponent = withRouter(OverlayHandler)

export default routedComponent

interface RouteOverlayProps {
  overlayID: OverlayID
}

export function RouteOverlay<P>(
  WrappedComponent: typeof routedComponent,
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
