import React, {SFC} from 'react'

interface Props {
  message: string
}

const EmptyGraphMessage: SFC<Props> = ({message}) => {
  return (
    <div className="graph-empty">
      <p>{message}</p>
    </div>
  )
}

export default EmptyGraphMessage
