// Libraries
import React, {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import queryString from 'query-string'
import {keys, pick} from 'lodash'

// Components
// All overlays in the product would be imported into this file
import NoteEditorOverlay from 'src/dashboards/components/NoteEditorOverlay'

const OverlayRouter: FunctionComponent<WithRouterProps> = ({
  location,
  router,
}) => {
  // I used queryString since internet explorer does not support the vanilla
  // way of doing this
  const searchParams = queryString.parse(location.search)
  const {overlay, resource} = searchParams
  let overlayID = ''
  let resourceID = ''

  // Coerce type into string
  // Had to implement this because search params can receive multiple values
  // for a single key. This approach only accepts the first value passed in
  // to help enforce a UI design principle that we should not have overalys
  // displayed on top of overlays (overlayception)
  //
  // That being said I think this could be refactored to accept multiple values
  // and just stack the overlays in the order they are passed in

  if (Array.isArray(overlay)) {
    overlayID = overlay[0]
  } else {
    overlayID = `${overlay}`
  }

  if (Array.isArray(resource)) {
    resourceID = resource[0]
  } else {
    resourceID = `${resource}`
  }

  // Goal here is to preserve whatever search params exist
  // so overlays are non-destructive
  const handleDismissOverlay = (): void => {
    const paramKeysWithoutOverlays = keys(searchParams).filter(
      pk => pk !== 'overlay' && pk !== 'resource'
    )

    let dismissPath = `${location.pathname}`

    if (paramKeysWithoutOverlays.length) {
      const cleanedParams = pick(searchParams, paramKeysWithoutOverlays)
      dismissPath = `${dismissPath}?${queryString.stringify(cleanedParams)}`
    }

    router.push(dismissPath)
  }

  let activeOverlay = <></>

  switch (overlayID) {
    case 'add-note':
      activeOverlay = <NoteEditorOverlay onDismiss={handleDismissOverlay} />
      break
    case 'edit-note':
      activeOverlay = (
        <NoteEditorOverlay
          onDismiss={handleDismissOverlay}
          cellID={resourceID}
        />
      )
      break
    default:
      break
  }

  // I opted for this approach rather than simply returning an overlay above
  // so that down the road I can wrap activeOverlay with a component that is
  // always rendered in the and can handle fading in/out overlays smoothly
  // when they mount/unmount

  return activeOverlay
}

export default withRouter<{}>(OverlayRouter)
