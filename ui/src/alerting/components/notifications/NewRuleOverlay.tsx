// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import RuleOverlayContents from 'src/alerting/components/notifications/RuleOverlayContents'
import {Overlay} from '@influxdata/clockface'

// Utils
import {RuleOverlayProvider} from './RuleOverlay.reducer'

// Constants
import {NEW_RULE_DRAFT} from 'src/alerting/constants'

type Props = WithRouterProps

const NewRuleOverlay: FC<Props> = ({params, router}) => {
  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  return (
    <RuleOverlayProvider initialState={NEW_RULE_DRAFT}>
      <Overlay visible={true}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Header
            title="Create a Notification Rule"
            onDismiss={handleDismiss}
          />
          <Overlay.Body>
            <RuleOverlayContents />
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    </RuleOverlayProvider>
  )
}

export default withRouter<Props>(NewRuleOverlay)
