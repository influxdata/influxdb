// Libraries
import React, {useMemo, FC} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Actions
import {createRule} from 'src/alerting/actions/notifications/rules'

// Components
import RuleOverlayContents from 'src/alerting/components/notifications/RuleOverlayContents'
import {Overlay} from '@influxdata/clockface'

// Utils
import {RuleOverlayProvider} from './RuleOverlay.reducer'
import {initRuleDraft} from 'src/alerting/components/notifications/utils'

// Types
import {NotificationRuleDraft} from 'src/types'

interface DispatchProps {
  onCreateRule: (rule: Partial<NotificationRuleDraft>) => Promise<void>
}

type Props = WithRouterProps & DispatchProps

const NewRuleOverlay: FC<Props> = ({params: {orgID}, router, onCreateRule}) => {
  const handleDismiss = () => {
    router.push(`/orgs/${orgID}/alerting`)
  }

  const handleCreateRule = async (rule: NotificationRuleDraft) => {
    await onCreateRule(rule)

    handleDismiss()
  }

  const initialState = useMemo(() => initRuleDraft(orgID), [orgID])

  return (
    <RuleOverlayProvider initialState={initialState}>
      <Overlay visible={true}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Header
            title="Create a Notification Rule"
            onDismiss={handleDismiss}
          />
          <Overlay.Body>
            <RuleOverlayContents
              saveButtonText="Create Notification Rule"
              onSave={handleCreateRule}
            />
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    </RuleOverlayProvider>
  )
}

const mdtp = {
  onCreateRule: createRule as any,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(withRouter<Props>(NewRuleOverlay))
