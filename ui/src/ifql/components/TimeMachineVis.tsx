import React, {SFC} from 'react'

interface Props {
  blob: string
}
const TimeMachineVis: SFC<Props> = ({blob}) => (
  <div className="time-machine-visualization">
    <div className="time-machine--graph">
      <div className="time-machine--graph-body">{blob}</div>
    </div>
  </div>
)

export default TimeMachineVis
