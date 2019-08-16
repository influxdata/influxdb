// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Overlay} from '@influxdata/clockface'
import {EndpointOverlayProvider} from 'src/alerting/components/endpoints/EndpointOverlayProvider'
import EndpointOverlayContents from 'src/alerting/components/endpoints/EndpointOverlayContents'

// Constants
import {NEW_ENDPOINT_DRAFT} from 'src/alerting/constants'

type Props = WithRouterProps

const NewRuleOverlay: FC<Props> = ({params, router}) => {
  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
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
          <EndpointOverlayContents />
        </Overlay.Container>
      </Overlay>
    </EndpointOverlayProvider>
  )
}

export default withRouter<Props>(NewRuleOverlay)
