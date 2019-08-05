// Libraries
import React, {FC} from 'react'

interface Props {
  row: {status: string}
}

const StatusTableField: FC<Props> = ({row: {status}}) => {
  return (
    <div
      className={`status-table-field status-table-field--${status.toLowerCase()}`}
    >
      {status}
    </div>
  )
}

export default StatusTableField
