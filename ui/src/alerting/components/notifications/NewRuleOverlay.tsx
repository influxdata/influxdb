// Libraries
import React, {FC, useReducer, useCallback} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import RuleOverlayContents from 'src/alerting/components/notifications/RuleOverlayContents'
import {Overlay} from '@influxdata/clockface'

// Reducers
import {reducer, ActionPayload} from './RuleOverlay.reducer'

// Constants
import {newRule} from 'src/alerting/constants'

// Context
import {RuleMode, NewRuleDispatch, RuleModeContext} from 'src/shared/hooks'

type Props = WithRouterProps

const NewRuleOverlay: FC<Props> = ({params, router}) => {
  const handleDismiss = () => {
    router.push(`/orgs/${params.orgID}/alerting`)
  }

  const mode = RuleMode.New
  const memoizedReducer = useCallback(reducer(mode), [])
  const [rule, dispatch] = useReducer(memoizedReducer, newRule)

  const ruleDispatch = (action: ActionPayload) => {
    dispatch({...action, mode})
  }

  return (
    <RuleModeContext.Provider value={RuleMode.New}>
      <NewRuleDispatch.Provider value={ruleDispatch}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={800}>
            <Overlay.Header
              title="Create a Notification Rule"
              onDismiss={handleDismiss}
            />
            <Overlay.Body>
              <RuleOverlayContents rule={rule} />
            </Overlay.Body>
          </Overlay.Container>
        </Overlay>
      </NewRuleDispatch.Provider>
    </RuleModeContext.Provider>
  )
}

export default withRouter<Props>(NewRuleOverlay)
