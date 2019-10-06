// Libraries
import React, {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import queryString from 'query-string'

// Components
import AllAccessTokenOverlay from 'src/authorizations/components/AllAccessTokenOverlay'
import BucketsTokenOverlay from 'src/authorizations/components/BucketsTokenOverlay'
import NewEndpointOverlay from 'src/alerting/components/endpoints/NewEndpointOverlay'
import DeleteDataOverlay from 'src/dataExplorer/components/DeleteDataOverlay'
import SaveAsOverlay from 'src/dataExplorer/components/SaveAsOverlay'

const OverlayRouter: FunctionComponent<WithRouterProps> = ({location, router}) => {
  const {overlay} = queryString.parse(location.search)

  
  const handleDismissOverlay = (): void => {
    const newPath = `${location.pathname}`
    router.push(newPath)
  }

  let activeOverlay = <></>

  switch (overlay) {
    case 'generate-all-access-token':
      activeOverlay = <AllAccessTokenOverlay onDismiss={handleDismissOverlay} />
      break
    case 'generate-read-write-token':
      activeOverlay = <BucketsTokenOverlay onDismiss={handleDismissOverlay} />
      break
    case 'new-endpoint':
      activeOverlay = <NewEndpointOverlay onDismiss={handleDismissOverlay} />
      break
    case 'delete-data':
      activeOverlay = <DeleteDataOverlay onDismiss={handleDismissOverlay} />
      break
    case 'save-as':
      activeOverlay = <SaveAsOverlay onDismiss={handleDismissOverlay} />
      break
    default:
      break
  }

  return activeOverlay
}



export default withRouter(OverlayRouter)
