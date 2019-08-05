// Libraries
import React, {FC} from 'react'
import moment from 'moment'

// Constants
import {DEFAULT_TIME_FORMAT} from 'src/shared/constants'

interface Props {
  row: {time: string | number}
}

const TimeTableField: FC<Props> = ({row: {time}}) => {
  return (
    <div className="time-table-field">
      {moment.utc(time).format(DEFAULT_TIME_FORMAT)}
    </div>
  )
}

export default TimeTableField
