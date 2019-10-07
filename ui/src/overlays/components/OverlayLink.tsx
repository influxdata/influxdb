// Libraries
import {FunctionComponent} from 'react'
import {withRouter, WithRouterProps, InjectedRouter} from 'react-router'

type HandleOpenOverlay = () => void

interface OwnProps {
  children: (onClick: HandleOpenOverlay) => JSX.Element
  overlayID: string
}

type Props = OwnProps & WithRouterProps

const generateOverlayURL = (pathname: string, overlayID: string): string => {
  return `${pathname}?overlay=${overlayID}`
}

const OverlayLink: FunctionComponent<Props> = ({children, location, router, overlayID}) => {
  const overlayURL = generateOverlayURL(location.pathname, overlayID)
  
  const handleClick = (): void => {
    router.push(overlayURL)
  }

  return children(handleClick)
}

export default withRouter(OverlayLink)

export const displayOverlay = (pathname: string, router: InjectedRouter, overlayID: string): HandleOpenOverlay => {
  const overlayURL = generateOverlayURL(pathname, overlayID)
  const displayOverlay = (): void => {
    router.push(overlayURL)
  }

  return displayOverlay
}