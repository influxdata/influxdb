// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Types
import {NotificationRule, AppState} from 'src/types'

interface StateProps {
  notificationRules: NotificationRule[]
}

type Props = StateProps

const NotificationRulesColumn: FunctionComponent<Props> = () => {
  return <>NotificationRules</>
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
