// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Types
import {NotificationRule, AppState} from 'src/types'
import NotificationRuleCards from 'src/alerting/components/NotificationRuleCards'

interface StateProps {
  notificationRules: NotificationRule[]
}

type Props = StateProps

const NotificationRulesColumn: FunctionComponent<Props> = ({
  notificationRules,
}) => {
  return (
    <>
      NotificationRules
      <NotificationRuleCards notificationRules={notificationRules} />
    </>
  )
}

const mstp = (state: AppState) => {
  const {
    notificationRules: {list: notificationRules},
  } = state

  return {notificationRules}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(NotificationRulesColumn)
