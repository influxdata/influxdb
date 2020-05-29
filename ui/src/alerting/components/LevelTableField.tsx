// Libraries
import React, {FC} from 'react'

// Types
import {StatusRow, NotificationRow} from 'src/types'

interface Props {
  row: StatusRow | NotificationRow
}

const LevelTableField: FC<Props> = ({row: {level}}) => {
  return (
    <div
      className={`level-table-field level-table-field--${level.toLowerCase()}`}
    >
      {level}
    </div>
  )
}

export default LevelTableField
