import React, {SFC} from 'react'

interface Props {
  message: string
  testID?: string
}

const EmptyGraphMessage: SFC<Props> = ({message, testID}) => {
  return (
    <div className="cell--view-empty" data-testid={testID}>
      <h4>{message}</h4>
    </div>
  )
}

export default EmptyGraphMessage
