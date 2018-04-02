import React, {SFC} from 'react'

interface Props {
  message: string
}

const DeprecationWarning: SFC<Props> = ({message}) => (
  <div>
    <span className="icon stop" /> {message}
  </div>
)

export default DeprecationWarning
