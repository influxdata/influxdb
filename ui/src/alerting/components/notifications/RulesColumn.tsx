// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Types
import {NotificationRule, AppState} from 'src/types'
import NotificationRuleCards from 'src/alerting/components/notifications/RuleCards'
import AlertsColumnHeader from 'src/alerting/components/AlertsColumnHeader'

interface StateProps {
  rules: NotificationRule[]
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
    <>
      <AlertsColumnHeader
        title="Notification Rules"
        onCreate={handleOpenOverlay}
      />
      <NotificationRuleCards rules={rules} />
    </>
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
