// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Types
import {NotificationRuleDraft, AppState} from 'src/types'

// Components
import NotificationRuleCards from 'src/alerting/components/notifications/RuleCards'
import AlertsColumn from 'src/alerting/components/AlertsColumn'

interface StateProps {
  rules: NotificationRuleDraft[]
}

type Props = StateProps & WithRouterProps

const NotificationRulesColumn: FunctionComponent<Props> = ({
  rules,
  router,
  params,
}) => {
  const handleOpenOverlay = () => {
    const newRuleRoute = `/orgs/${params.orgID}/alerting/rules/new`
    router.push(newRuleRoute)
  }

  return (
    <AlertsColumn
      title="Notification Rules"
      testID="create-rule"
      onCreate={handleOpenOverlay}
    >
      <NotificationRuleCards rules={rules} />
    </AlertsColumn>
  )
}

const mstp = (state: AppState) => {
  const {
    rules: {list: rules},
  } = state

  return {rules}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(withRouter(NotificationRulesColumn))
