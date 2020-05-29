import React, {CSSProperties, FC} from 'react'

interface Props {
  style: CSSProperties
  index: number
}

const ErrorRow: FC<Props> = ({style, index}) => {
  return (
    <div style={style}>
      <div className="event-error-row">
        {index === 0 ? 'Failed to load :(' : 'Failed to load next rows :('}
      </div>
    </div>
  )
}

export default ErrorRow
