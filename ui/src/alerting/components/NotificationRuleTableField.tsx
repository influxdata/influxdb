// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'

// Selectors
import {getRuleIDs} from 'src/alerting/reducers/notifications/rules'

// Types
import {NotificationRow, AppState} from 'src/types'

interface OwnProps {
  row: NotificationRow
}

interface StateProps {
  ruleIDs: {[x: string]: boolean}
}

type Props = OwnProps & StateProps

const NotificationRuleTableField: FC<Props> = ({
  row: {notificationRuleName, notificationRuleID},
  ruleIDs,
}) => {
  if (!ruleIDs[notificationRuleID]) {
    return (
      <div
        className="rule-name-field"
        title="The rule that created this no longer exists"
      >
        {notificationRuleName}
      </div>
    )
  }
  const href = formatOrgRoute(`/alerting/rules/${notificationRuleID}/edit`)

  return <Link to={href}>{notificationRuleName}</Link>
}

const mstp = (state: AppState) => {
  return {
    ruleIDs: getRuleIDs(state.rules),
  }
}
export default connect<StateProps>(mstp)(NotificationRuleTableField)
