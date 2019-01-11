// Libraries
import React, {SFC} from 'react'

interface Props {
  labels: JSX.Element | JSX.Element[]
}

const LabelTooltip: SFC<Props> = ({labels}) => (
  <div className="label--tooltip">
    <div className="label--tooltip-container">{labels}</div>
  </div>
)

export default LabelTooltip
