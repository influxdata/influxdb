// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Types
import {NotificationRule, AppState} from 'src/types'
import NotificationRuleCards from 'src/alerting/components/notifications/RuleCards'
import AlertsColumnHeader from 'src/alerting/components/AlertsColumnHeader'

interface StateProps {
  rules: NotificationRule[]
}

type Props = StateProps

const NotificationRulesColumn: FunctionComponent<Props> = ({rules}) => {
  const handleClick = () => {}

  return (
    <>
      <AlertsColumnHeader title="Notification Rules" onCreate={handleClick} />
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
)(NotificationRulesColumn)
