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
import UpdateBucketOverlay from 'src/buckets/components/UpdateBucketOverlay'
import RenameBucketOverlay from 'src/buckets/components/RenameBucketOverlay'
import DashboardImportOverlay from 'src/dashboards/components/DashboardImportOverlay'
import CreateFromTemplateOverlay from 'src/templates/components/createFromTemplateOverlay/CreateFromTemplateOverlay'
import DashboardExportOverlay from 'src/dashboards/components/DashboardExportOverlay'
import RenameOrgOverlay from 'src/organizations/components/RenameOrgOverlay'
import EditEndpointOverlay from 'src/alerting/components/endpoints/EditEndpointOverlay'
import NewRuleOverlay from 'src/alerting/components/notifications/NewRuleOverlay'
import EditRuleOverlay from 'src/alerting/components/notifications/EditRuleOverlay'
import NewThresholdCheckEO from 'src/alerting/components/NewThresholdCheckEO'
import NewDeadmanCheckEO from 'src/alerting/components/NewDeadmanCheckEO'
import EditCheckEO from 'src/alerting/components/EditCheckEO'
import CreateOrgOverlay from 'src/organizations/components/CreateOrgOverlay'
import TemplateImportOverlay from 'src/templates/components/TemplateImportOverlay'
import TemplateExportOverlay from 'src/templates/components/TemplateExportOverlay'
import TemplateViewOverlay from 'src/templates/components/TemplateViewOverlay'
import StaticTemplateViewOverlay from 'src/templates/components/StaticTemplateViewOverlay'

const OverlayRouter: FunctionComponent<WithRouterProps> = ({
  location,
  router,
}) => {
  const {overlay, resource} = queryString.parse(location.search)
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
    case 'create-threshold-check':
      activeOverlay = <NewThresholdCheckEO onDismiss={handleDismissOverlay} />
      break
    case 'create-deadman-check':
      activeOverlay = <NewDeadmanCheckEO onDismiss={handleDismissOverlay} />
      break
    case 'edit-check':
      activeOverlay = (
        <EditCheckEO onDismiss={handleDismissOverlay} checkID={resourceID} />
      )
      break
    case 'create-endpoint':
      activeOverlay = <NewEndpointOverlay onDismiss={handleDismissOverlay} />
      break
    case 'edit-endpoint':
      activeOverlay = (
        <EditEndpointOverlay
          onDismiss={handleDismissOverlay}
          endpointID={resourceID}
        />
      )
      break
    case 'create-notification-rule':
      activeOverlay = <NewRuleOverlay onDismiss={handleDismissOverlay} />
      break
    case 'edit-notification-rule':
      activeOverlay = (
        <EditRuleOverlay onDismiss={handleDismissOverlay} ruleID={resourceID} />
      )
      break
    case 'delete-data':
      activeOverlay = (
        <DeleteDataOverlay
          onDismiss={handleDismissOverlay}
          bucketID={resourceID}
        />
      )
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
    case 'edit-bucket':
      activeOverlay = (
        <UpdateBucketOverlay
          onDismiss={handleDismissOverlay}
          bucketID={resourceID}
        />
      )
      break
    case 'rename-bucket':
      activeOverlay = (
        <RenameBucketOverlay
          onDismiss={handleDismissOverlay}
          bucketID={resourceID}
        />
      )
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
    case 'rename-organization':
      activeOverlay = <RenameOrgOverlay onDismiss={handleDismissOverlay} />
      break
    case 'create-organization':
      activeOverlay = <CreateOrgOverlay onDismiss={handleDismissOverlay} />
      break
    case 'import-template':
      activeOverlay = <TemplateImportOverlay onDismiss={handleDismissOverlay} />
      break
    case 'export-template':
      activeOverlay = (
        <TemplateExportOverlay
          onDismiss={handleDismissOverlay}
          templateID={resourceID}
        />
      )
      break
    case 'view-user-template':
      activeOverlay = (
        <TemplateViewOverlay
          onDismiss={handleDismissOverlay}
          templateID={resourceID}
        />
      )
      break
    case 'view-static-template':
      activeOverlay = (
        <StaticTemplateViewOverlay
          onDismiss={handleDismissOverlay}
          templateID={resourceID}
        />
      )
      break
    default:
      break
  }

  return activeOverlay
}

export default withRouter<{}>(OverlayRouter)
