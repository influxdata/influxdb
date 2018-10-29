// Libraries
import React, {SFC} from 'react'

interface Props {
  text: string
}

const EmptyStateSubText: SFC<Props> = ({text}) => (
  <p className="empty-state--sub-text">{text}</p>
)

export default EmptyStateSubText
