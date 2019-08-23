// Libraries
import React, {FC} from 'react'
import moment from 'moment'

// Constants
import {DEFAULT_TIME_FORMAT} from 'src/shared/constants'

// Types
import {StatusRow, NotificationRow} from 'src/types'

interface Props {
  row: StatusRow | NotificationRow
}

const TimeTableField: FC<Props> = ({row: {time}}) => {
  return (
    <div className="time-table-field">
      {moment.utc(time).format(DEFAULT_TIME_FORMAT)}
    </div>
  )
}

export default TimeTableField
