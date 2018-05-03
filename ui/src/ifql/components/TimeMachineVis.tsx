import React, {SFC} from 'react'

interface Props {
  blob: string
}
const TimeMachineVis: SFC<Props> = ({blob}) => (
  <div className="time-machine-visualization">
    <div className="time-machine--graph">
      <div className="time-machine--graph-header">
        <ul className="nav nav-tablist nav-tablist-sm">
          <li className="active">Line Graph</li>
          <li>Table</li>
        </ul>
      </div>
      <div className="time-machine--graph-body">{`I AM A ${blob} GRAPH`}</div>
    </div>
  </div>
)

export default TimeMachineVis
