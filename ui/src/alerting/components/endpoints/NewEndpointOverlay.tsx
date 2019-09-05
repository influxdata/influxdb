// Libraries
import React, {FC, useMemo} from 'react'
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
  const {orgID} = params
  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  const handleCreateEndpoint = async (endpoint: NotificationEndpoint) => {
    await onCreateEndpoint(endpoint)

    handleDismiss()
  }

  const initialState = useMemo(() => ({...NEW_ENDPOINT_DRAFT, orgID}), [orgID])

  return (
    <EndpointOverlayProvider initialState={initialState}>
      <Overlay visible={true}>
        <Overlay.Container maxWidth={666}>
          <Overlay.Header
            title="Create a Notification Endpoint"
            onDismiss={handleDismiss}
          />
          <EndpointOverlayContents
            onSave={handleCreateEndpoint}
            onCancel={handleDismiss}
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
