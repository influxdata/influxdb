// Libraries
import React, {SFC} from 'react'

interface Props {
  text: string
}

const EmptyStateText: SFC<Props> = ({text}) => (
  <h4 className="empty-state--text">{text}</h4>
)

export default EmptyStateText
