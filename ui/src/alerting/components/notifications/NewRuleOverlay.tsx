// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Overlay} from '@influxdata/clockface'

type Props = WithRouterProps

const NewRuleOverlay: FC<Props> = ({params, router}) => {
  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  return (
    <Overlay visible={true}>
      <Overlay.Container>
        <Overlay.Header
          title="Create a Notification Rule"
          onDismiss={handleDismiss}
        />
        <Overlay.Body>hai</Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

export default withRouter<Props>(NewRuleOverlay)
