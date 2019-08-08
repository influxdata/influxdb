// Libraries
import React, {FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {Overlay} from '@influxdata/clockface'
import RuleOverlayContents from 'src/alerting/components/notifications/RuleOverlayContents'

// Reducers
import {memoizedReducer, ActionPayload} from './RuleOverlay.reducer'

// Constants
import {RuleModeContext, EditRuleDispatch, RuleMode} from 'src/shared/hooks'

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

  const mode = RuleMode.Edit
  const [rule, dispatch] = memoizedReducer(mode, stateRule)
  const ruleDispatch = (action: ActionPayload): void => {
    dispatch({...action, mode})
  }

  return (
    <RuleModeContext.Provider value={mode}>
      <EditRuleDispatch.Provider value={ruleDispatch}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={800}>
            <Overlay.Header
              title="Edit this Notification Rule"
              onDismiss={handleDismiss}
            />
            <Overlay.Body>
              <RuleOverlayContents rule={rule} />
            </Overlay.Body>
          </Overlay.Container>
        </Overlay>
      </EditRuleDispatch.Provider>
    </RuleModeContext.Provider>
  )
}

const mstp = ({rules}: AppState, {params}: Props): StateProps => {
  const stateRule = rules.list.find(r => r.id === params.ruleID)

  return {
    stateRule,
  }
}

export default connect<StateProps>(mstp)(withRouter<Props>(EditRuleOverlay))
