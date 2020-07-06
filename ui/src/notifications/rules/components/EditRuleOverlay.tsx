// Libraries
import React, {FC} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Constants
import {getNotificationRuleFailed} from 'src/shared/copy/notifications'

// Components
import {Overlay} from '@influxdata/clockface'
import RuleOverlayContents from 'src/notifications/rules/components/RuleOverlayContents'

// Actions
import {updateRule} from 'src/notifications/rules/actions/thunks'
import {notify} from 'src/shared/actions/notifications'

// Utils
import RuleOverlayProvider from './RuleOverlayProvider'
import {getByID} from 'src/resources/selectors'

// Types
import {NotificationRuleDraft, AppState, ResourceType} from 'src/types'

interface StateProps {
  stateRule: NotificationRuleDraft
}

interface DispatchProps {
  onNotify: typeof notify
  onUpdateRule: typeof updateRule
}

type Props = RouteComponentProps<{orgID: string; ruleID: string}> &
  StateProps &
  DispatchProps

const EditRuleOverlay: FC<Props> = ({
  match,
  history,
  stateRule,
  onUpdateRule,
  onNotify,
}) => {
  const handleDismiss = () => {
    history.push(`/orgs/${match.params.orgID}/alerting`)
  }

  if (!stateRule) {
    onNotify(getNotificationRuleFailed(match.params.ruleID))
    handleDismiss()
    return null
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
            testID="dismiss-overlay"
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

const mstp = (state: AppState, {match}: Props): StateProps => {
  const {ruleID} = match.params

  const stateRule = getByID<NotificationRuleDraft>(
    state,
    ResourceType.NotificationRules,
    ruleID
  )

  return {
    stateRule,
  }
}

const mdtp = {
  onNotify: notify,
  onUpdateRule: updateRule,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter(EditRuleOverlay))
