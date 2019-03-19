import React, {SFC} from 'react'

interface Props {
  text: string
  className?: string
}

const WaitingText: SFC<Props> = ({text, className}) => {
  return <div className={`waiting-text ${className || ''}`}>{text}</div>
}

export default WaitingText
