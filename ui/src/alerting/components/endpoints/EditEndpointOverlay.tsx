// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Actions
import {updateEndpoint} from 'src/alerting/actions/notifications/endpoints'

// Components
import {Overlay} from '@influxdata/clockface'
import {EndpointOverlayProvider} from 'src/alerting/components/endpoints/EndpointOverlayProvider'
import EndpointOverlayContents from 'src/alerting/components/endpoints/EndpointOverlayContents'

// Types
import {NotificationEndpoint, AppState} from 'src/types'

interface DispatchProps {
  onUpdateEndpoint: typeof updateEndpoint
}

interface StateProps {
  endpoint: NotificationEndpoint
}

type Props = WithRouterProps & DispatchProps & StateProps

const EditEndpointOverlay: FC<Props> = ({
  params,
  router,
  onUpdateEndpoint,
  endpoint: initialState,
}) => {
  const {orgID} = params
  const handleDismiss = () => {
    router.push(`/orgs/${orgID}/alerting`)
  }

  const handleEditEndpoint = async (endpoint: NotificationEndpoint) => {
    await onUpdateEndpoint(endpoint)

    handleDismiss()
  }

  return (
    <EndpointOverlayProvider initialState={initialState}>
      <Overlay visible={true}>
        <Overlay.Container maxWidth={600}>
          <Overlay.Header
            title="Edit a Notification Endpoint"
            onDismiss={handleDismiss}
          />
          <Overlay.Body />
          <EndpointOverlayContents
            onSave={handleEditEndpoint}
            onCancel={handleDismiss}
            saveButtonText="Edit Notification Endpoint"
          />
        </Overlay.Container>
      </Overlay>
    </EndpointOverlayProvider>
  )
}

const mdtp = {
  onUpdateEndpoint: updateEndpoint,
}

const mstp = ({endpoints}: AppState, {params}: Props): StateProps => {
  const endpoint = endpoints.list.find(ep => ep.id === params.endpointID)

  if (!endpoint) {
    throw new Error('Unknown endpoint provided to <EditEndpointOverlay/>')
  }

  return {endpoint}
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<Props>(EditEndpointOverlay))
