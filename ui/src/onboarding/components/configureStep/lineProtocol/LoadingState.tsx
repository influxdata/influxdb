// Libraries
import React, {SFC} from 'react'

// Types
import {LineProtocolStatus} from 'src/types/v2/dataLoaders'

interface Props {
  activeCard: LineProtocolStatus
}

const LoadingState: SFC<Props> = ({activeCard}) => {
  return <>{activeCard}</>
}

export default LoadingState
