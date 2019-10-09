// Libraries
import {FunctionComponent} from 'react'
import {withRouter, WithRouterProps, InjectedRouter} from 'react-router'
// import queryString from 'query-string'

type HandleOpenOverlay = () => void

interface OwnProps {
  children: (onClick: HandleOpenOverlay) => JSX.Element
  overlayID: string
  resourceID?: string
}

type Props = OwnProps & WithRouterProps

const generateOverlayURL = (
  pathname: string,
  overlayID: string,
  resourceID?: string,
  search?: string, 
): string => {
  let url = `${pathname}`

  if (search) {
    url = `${url}${search}&overlay=${overlayID}`
  } else {
    url = `${url}?overlay=${overlayID}`
  }

  if (resourceID) {
    url = `${url}&resource=${resourceID}`
  }

  return url
}

const OverlayLink: FunctionComponent<Props> = ({
  children,
  location,
  router,
  overlayID,
  resourceID,
}) => {
  const overlayURL = generateOverlayURL(
    location.pathname,
    overlayID,
    resourceID,
    location.search,
  )

  const handleClick = (): void => {
    router.push(overlayURL)
  }

  return children(handleClick)
}

export default withRouter(OverlayLink)

export const displayOverlay = (
  pathname: string,
  router: InjectedRouter,
  overlayID: string,
  resourceID?: string
): HandleOpenOverlay => {
  const overlayURL = generateOverlayURL(pathname, overlayID, resourceID)
  const displayOverlay = (): void => {
    router.push(overlayURL)
  }

  return displayOverlay
}
