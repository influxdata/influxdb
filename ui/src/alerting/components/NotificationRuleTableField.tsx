// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'

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
  const href = formatOrgRoute(`/alerting/rules/${notificationRuleID}/edit`)

  return <Link to={href}>{notificationRuleName}</Link>
}

export default NotificationRuleTableField
