import React, {SFC} from 'react'

interface Props {
  message: string
}

const DeprecationWarning: SFC<Props> = ({message}) => (
  <div className="alert alert-primary">
    <span className="icon stop" />
    <div className="alert-message">{message}</div>
  </div>
)

export default DeprecationWarning
