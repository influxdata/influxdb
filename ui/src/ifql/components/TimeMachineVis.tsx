import React, {SFC} from 'react'

interface Props {
  blob: string
}
const TimeMachineVis: SFC<Props> = ({blob}) => <div>{blob}</div>

export default TimeMachineVis
