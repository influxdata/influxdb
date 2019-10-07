// Libraries
import React, {FC} from 'react'
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

interface OwnProps {
  onDismiss: () => void
  ruleID: string
}

type Props = OwnProps & StateProps & DispatchProps

const EditRuleOverlay: FC<Props> = ({stateRule, onUpdateRule, onDismiss}) => {
  if (!stateRule) {
    return null
  }

  const handleUpdateRule = async (rule: NotificationRuleDraft) => {
    await onUpdateRule(rule)

    onDismiss()
  }

  return (
    <RuleOverlayProvider initialState={stateRule}>
      <Overlay visible={true}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Header
            title="Edit this Notification Rule"
            onDismiss={onDismiss}
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

const mstp = ({rules}: AppState, {ruleID}: OwnProps): StateProps => {
  const stateRule = rules.list.find(r => r.id === ruleID)

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
)(EditRuleOverlay)
