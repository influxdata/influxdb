// Libraries
import React, {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import queryString from 'query-string'
import {keys, pick} from 'lodash'

// Components
import NoteEditorOverlay from 'src/dashboards/components/NoteEditorOverlay'

const OverlayRouter: FunctionComponent<WithRouterProps> = ({
  location,
  router,
}) => {
  const queryParams = queryString.parse(location.search)
  const {overlay, resource} = queryParams
  let overlayID = ''
  let resourceID = ''

  // Coerce type into string
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

  // TODO: make this less destructive
  const handleDismissOverlay = (): void => {
    const paramKeysWithoutOverlays = keys(queryParams).filter(
      pk => pk !== 'overlay' && pk !== 'resource'
    )

    let dismissPath = `${location.pathname}`

    if (paramKeysWithoutOverlays.length) {
      const cleanedParams = pick(queryParams, paramKeysWithoutOverlays)
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

  return activeOverlay
}

export default withRouter<{}>(OverlayRouter)
