// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Actions
import {createEndpoint} from 'src/alerting/actions/notifications/endpoints'

// Components
import {Overlay} from '@influxdata/clockface'
import {EndpointOverlayProvider} from 'src/alerting/components/endpoints/EndpointOverlayProvider'
import EndpointOverlayContents from 'src/alerting/components/endpoints/EndpointOverlayContents'

// Constants
import {NEW_ENDPOINT_DRAFT} from 'src/alerting/constants'
import {NotificationEndpoint} from 'src/types'

interface DispatchProps {
  onCreateEndpoint: typeof createEndpoint
}

type Props = WithRouterProps & DispatchProps

const NewRuleOverlay: FC<Props> = ({params, router, onCreateEndpoint}) => {
  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  const handleCreateEndpoint = async (endpoint: NotificationEndpoint) => {
    await onCreateEndpoint(endpoint)

    handleDismiss()
  }

  return (
    <EndpointOverlayProvider initialState={NEW_ENDPOINT_DRAFT}>
      <Overlay visible={true}>
        <Overlay.Container maxWidth={600}>
          <Overlay.Header
            title="Create a Notification Endpoint"
            onDismiss={handleDismiss}
          />
          <Overlay.Body />
          <EndpointOverlayContents
            onSave={handleCreateEndpoint}
            saveButtonText="Create Notification Endpoint"
          />
        </Overlay.Container>
      </Overlay>
    </EndpointOverlayProvider>
  )
}

const mdtp = {
  onCreateEndpoint: createEndpoint,
}

export default connect<null, DispatchProps>(
  null,
  mdtp
)(withRouter<Props>(NewRuleOverlay))
