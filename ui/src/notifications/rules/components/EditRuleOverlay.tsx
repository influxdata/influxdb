// Libraries
import React, {FC} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'

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

type ReduxProps = ConnectedProps<typeof connector>
type RouterProps = RouteComponentProps<{orgID: string; ruleID: string}>
type Props = RouterProps & ReduxProps

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

const mstp = (state: AppState, {match}: RouterProps) => {
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

const connector = connect(mstp, mdtp)

export default connector(withRouter(EditRuleOverlay))
