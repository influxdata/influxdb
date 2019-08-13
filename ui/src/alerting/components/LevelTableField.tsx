// Libraries
import React, {FC} from 'react'

interface Props {
  row: {level: string}
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
