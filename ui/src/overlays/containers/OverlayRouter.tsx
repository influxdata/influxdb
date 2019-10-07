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
import AddMembersOverlay from 'src/members/components/AddMembersOverlay'
import ClientCSharpOverlay from 'src/clientLibraries/components/ClientCSharpOverlay'
import ClientGoOverlay from 'src/clientLibraries/components/ClientGoOverlay'
import ClientJavaOverlay from 'src/clientLibraries/components/ClientJavaOverlay'
import ClientJSOverlay from 'src/clientLibraries/components/ClientJSOverlay'
import ClientPythonOverlay from 'src/clientLibraries/components/ClientPythonOverlay'
import CreateBucketOverlay from 'src/buckets/components/CreateBucketOverlay'
import DashboardImportOverlay from 'src/dashboards/components/DashboardImportOverlay'
import CreateFromTemplateOverlay from 'src/templates/components/createFromTemplateOverlay/CreateFromTemplateOverlay'
import DashboardExportOverlay from 'src/dashboards/components/DashboardExportOverlay'

const OverlayRouter: FunctionComponent<WithRouterProps> = ({
  location,
  router,
}) => {
  const {overlay, resource} = queryString.parse(location.search)
  let overlayID = ''
  let resourceID = ''

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

  console.log(queryString.parse(location.search))

  const handleDismissOverlay = (): void => {
    const newPath = `${location.pathname}`
    router.push(newPath)
  }

  let activeOverlay = <></>

  switch (overlayID) {
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
    case 'add-members':
      activeOverlay = <AddMembersOverlay onDismiss={handleDismissOverlay} />
      break
    case 'csharp-client':
      activeOverlay = <ClientCSharpOverlay onDismiss={handleDismissOverlay} />
      break
    case 'go-client':
      activeOverlay = <ClientGoOverlay onDismiss={handleDismissOverlay} />
      break
    case 'java-client':
      activeOverlay = <ClientJavaOverlay onDismiss={handleDismissOverlay} />
      break
    case 'javascript-node-client':
      activeOverlay = <ClientJSOverlay onDismiss={handleDismissOverlay} />
      break
    case 'python-client':
      activeOverlay = <ClientPythonOverlay onDismiss={handleDismissOverlay} />
      break
    case 'create-bucket':
      activeOverlay = <CreateBucketOverlay onDismiss={handleDismissOverlay} />
      break
    case 'import-dashboard':
      activeOverlay = (
        <DashboardImportOverlay onDismiss={handleDismissOverlay} />
      )
      break
    case 'create-dashboard-from-template':
      activeOverlay = (
        <CreateFromTemplateOverlay onDismiss={handleDismissOverlay} />
      )
      break
    case 'export-dashboard':
      activeOverlay = (
        <DashboardExportOverlay
          onDismiss={handleDismissOverlay}
          dashboardID={resourceID}
        />
      )
      break
    default:
      break
  }

  return activeOverlay
}

export default withRouter<{}>(OverlayRouter)
