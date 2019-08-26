// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {Overlay} from '@influxdata/clockface'
import RuleOverlayContents from 'src/alerting/components/notifications/RuleOverlayContents'

// Actions
import {updateRule} from 'src/alerting/actions/notifications/rules'

// Utils
import RuleOverlayProvider from './RuleOverlayProvider'

// Types
import {NotificationRuleDraft, AppState} from 'src/types'

interface StateProps {
  stateRule: NotificationRuleDraft
}

interface DispatchProps {
  onUpdateRule: (rule: NotificationRuleDraft) => Promise<void>
}

type Props = WithRouterProps & StateProps & DispatchProps

const EditRuleOverlay: FC<Props> = ({
  params: {orgID},
  router,
  stateRule,
  onUpdateRule,
}) => {
  if (!stateRule) {
    return null
  }

  const handleDismiss = () => {
    router.push(`/orgs/${orgID}/alerting`)
  }

  const handleUpdateRule = async (rule: NotificationRuleDraft) => {
    await onUpdateRule(rule)

    handleDismiss()
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
            <RuleOverlayContents
              saveButtonText="Save Changes"
              onSave={handleUpdateRule}
            />
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

const mdtp = {
  onUpdateRule: updateRule as any,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<Props>(EditRuleOverlay))
