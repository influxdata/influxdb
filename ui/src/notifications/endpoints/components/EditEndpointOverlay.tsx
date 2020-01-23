// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Constants
import {getEndpointFailed} from 'src/shared/copy/notifications'

// Actions
import {updateEndpoint} from 'src/notifications/endpoints/actions'
import {notify} from 'src/shared/actions/notifications'

// Components
import {Overlay} from '@influxdata/clockface'
import {EndpointOverlayProvider} from 'src/notifications/endpoints/components/EndpointOverlayProvider'
import EndpointOverlayContents from 'src/notifications/endpoints/components/EndpointOverlayContents'

// Types
import {NotificationEndpoint, AppState} from 'src/types'

interface DispatchProps {
  onUpdateEndpoint: typeof updateEndpoint
  onNotify: typeof notify
}

interface StateProps {
  endpoint: NotificationEndpoint
}

type Props = WithRouterProps & DispatchProps & StateProps

const EditEndpointOverlay: FC<Props> = ({
  params,
  router,
  onUpdateEndpoint,
  onNotify,
  endpoint,
}) => {
  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  if (!endpoint) {
    onNotify(getEndpointFailed(params.endpointID))
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

const mstp = ({endpoints}: AppState, {params}: Props): StateProps => {
  const endpoint = endpoints.list.find(ep => ep.id === params.endpointID)

  return {endpoint}
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<Props>(EditEndpointOverlay))
