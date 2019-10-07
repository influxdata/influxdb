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
import RuleOverlayProvider from 'src/alerting/components/notifications/RuleOverlayProvider'
import {initRuleDraft} from 'src/alerting/components/notifications/utils'

// Types
import {NotificationRuleDraft} from 'src/types'

interface DispatchProps {
  onCreateRule: (rule: Partial<NotificationRuleDraft>) => Promise<void>
}

interface OwnProps {
  onDismiss: () => void
}

type Props = OwnProps & WithRouterProps & DispatchProps

const NewRuleOverlay: FC<Props> = ({
  params: {orgID},
  onCreateRule,
  onDismiss,
}) => {
  const handleCreateRule = async (rule: NotificationRuleDraft) => {
    await onCreateRule(rule)

    onDismiss()
  }

  const initialState = useMemo(() => initRuleDraft(orgID), [orgID])

  return (
    <RuleOverlayProvider initialState={initialState}>
      <Overlay visible={true}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Header
            title="Create a Notification Rule"
            onDismiss={onDismiss}
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
)(withRouter<OwnProps>(NewRuleOverlay))
