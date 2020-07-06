// Libraries
import React, {FC, useContext} from 'react'
import {Link} from 'react-router-dom'

// Context
import {ResourceIDsContext} from 'src/alerting/components/AlertHistoryIndex'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'

// Types
import {NotificationRow} from 'src/types'

interface Props {
  row: NotificationRow
}

const NotificationRuleTableField: FC<Props> = ({
  row: {notificationRuleName, notificationRuleID},
}) => {
  const {ruleIDs} = useContext(ResourceIDsContext)
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

export default NotificationRuleTableField
