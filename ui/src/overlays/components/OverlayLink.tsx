// Libraries
import {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

type HandleOpenOverlay = () => void

interface OwnProps {
  children: (onClick: HandleOpenOverlay) => JSX.Element
  overlayID: string
  resourceID?: string
}

type Props = OwnProps & WithRouterProps

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
    location.search
  )

  const handleClick = (): void => {
    router.push(overlayURL)
  }
  
  // I opted to handle passing this to the child via render prop so it's
  // more apparent how it works when using the component.
  // Alternatively we could have added/removed an event listener to the child
  // element on mount/unmount, or used cloneElement with the prop.
  // Render prop seemed simplest even if it means the API for this component
  // is slightly bulkier

  return children(handleClick)
}

export default withRouter(OverlayLink)

const generateOverlayURL = (
  pathname: string,
  overlayID: string,
  resourceID?: string,
  search?: string
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
