// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Constants
import {getEndpointFailed} from 'src/shared/copy/notifications'

// Actions
import {updateEndpoint} from 'src/notifications/endpoints/actions/thunks'
import {notify} from 'src/shared/actions/notifications'

// Components
import {Overlay} from '@influxdata/clockface'
import {EndpointOverlayProvider} from 'src/notifications/endpoints/components/EndpointOverlayProvider'
import EndpointOverlayContents from 'src/notifications/endpoints/components/EndpointOverlayContents'

// Types
import {NotificationEndpoint, AppState, ResourceType} from 'src/types'

// Utils
import {getByID} from 'src/resources/selectors'

interface DispatchProps {
  onUpdateEndpoint: typeof updateEndpoint
  onNotify: typeof notify
}

interface StateProps {
  endpoint: NotificationEndpoint
}

type Props = RouteComponentProps<{orgID: string; endpointID: string}> &
  DispatchProps &
  StateProps

const EditEndpointOverlay: FC<Props> = ({
  match,
  history,
  onUpdateEndpoint,
  onNotify,
  endpoint,
}) => {
  const handleDismiss = () => {
    history.push(`/orgs/${match.params.orgID}/alerting`)
  }

  if (!endpoint) {
    onNotify(getEndpointFailed(match.params.endpointID))
    handleDismiss()
    return null
  }

  const handleEditEndpoint = (endpoint: NotificationEndpoint) => {
    onUpdateEndpoint(endpoint)

    handleDismiss()
  }

  return (
    <EndpointOverlayProvider initialState={endpoint}>
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
  onNotify: notify,
}

const mstp = (state: AppState, {match}: Props): StateProps => {
  const endpoint = getByID<NotificationEndpoint>(
    state,
    ResourceType.NotificationEndpoints,
    match.params.endpointID
  )

  return {endpoint}
}

export default withRouter(
  connect<StateProps, DispatchProps, Props>(mstp, mdtp)(EditEndpointOverlay)
)
