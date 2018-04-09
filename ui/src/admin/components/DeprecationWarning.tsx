import React, {SFC} from 'react'

interface Props {
  message: string | JSX.Element
}

const DeprecationWarning: SFC<Props> = ({message}) => (
  <div className="alert alert-primary">
    <span className="icon octagon" />
    <div className="alert-message">{message}</div>
  </div>
)

export default DeprecationWarning
