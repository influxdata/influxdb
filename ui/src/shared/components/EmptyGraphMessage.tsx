import React, {SFC} from 'react'

interface Props {
  message: string
  type?: string
}

export enum GraphMessageType {
  Empty = 'empty',
  Error = 'error',
  NoData = 'no-data',
  RemoteDataState = 'remote-data-state',
}

const EmptyGraphMessage: SFC<Props> = ({
  message,
  type = GraphMessageType.Empty,
}) => {
  return (
    <div
      className="cell--view-empty"
      data-testid={`empty-graph-message ${type}`}
    >
      <h4>{message}</h4>
    </div>
  )
}

export default EmptyGraphMessage
