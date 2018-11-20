import React, {SFC} from 'react'

interface Props {
  message: string
}

const EmptyGraphMessage: SFC<Props> = ({message}) => {
  return (
    <div className="cell--view-empty">
      <h4>{message}</h4>
    </div>
  )
}

export default EmptyGraphMessage
