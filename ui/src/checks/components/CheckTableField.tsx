// Libraries
import React, {FC, useContext} from 'react'
import {Link} from 'react-router-dom'

// Context
import {ResourceIDsContext} from 'src/alerting/components/AlertHistoryIndex'

// Utils
import {formatOrgRoute} from 'src/shared/utils/formatOrgRoute'

// Types
import {StatusRow, NotificationRow} from 'src/types'

interface Props {
  row: StatusRow | NotificationRow
}

const CheckTableField: FC<Props> = ({row: {checkName, checkID}}) => {
  const {checkIDs} = useContext(ResourceIDsContext)

  if (!checkIDs[checkID]) {
    return (
      <div
        className="check-name-field"
        title="The check that created this no longer exists"
      >
        {checkName}
      </div>
    )
  }

  const href = formatOrgRoute(`/alerting/checks/${checkID}/edit`)

  return <Link to={href}>{checkName}</Link>
}

export default CheckTableField
