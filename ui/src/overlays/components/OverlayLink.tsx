// Libraries
import {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

type HandleOpenOverlay = () => void

interface OwnProps {
  children: (onClick: HandleOpenOverlay) => JSX.Element
  overlayID: string
}

type Props = OwnProps & WithRouterProps

const OverlayLink: FunctionComponent<Props> = ({children, location, router, overlayID}) => {
  const overlayURL = `${location.pathname}?overlay=${overlayID}`
  
  const handleClick = (): void => {
    router.push(overlayURL)
  }

  return children(handleClick)
}

export default withRouter(OverlayLink)