// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {Overlay} from '@influxdata/clockface'
import RuleOverlayContents from 'src/alerting/components/notifications/RuleOverlayContents'

// Utils
import {RuleOverlayProvider} from './RuleOverlay.reducer'

// Types
import {NotificationRuleDraft, AppState} from 'src/types'

interface StateProps {
  stateRule: NotificationRuleDraft
}

type Props = WithRouterProps & StateProps

const EditRuleOverlay: FC<Props> = ({params, router, stateRule}) => {
  if (!stateRule) {
    return null
  }

  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  return (
    <RuleOverlayProvider initialState={stateRule}>
      <Overlay visible={true}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Header
            title="Edit this Notification Rule"
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

const mstp = ({rules}: AppState, {params}: Props): StateProps => {
  const stateRule = rules.list.find(r => r.id === params.ruleID)

  return {
    stateRule,
  }
}

export default connect<StateProps>(mstp)(withRouter<Props>(EditRuleOverlay))
