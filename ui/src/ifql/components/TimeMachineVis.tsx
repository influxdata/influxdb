import React, {SFC} from 'react'

interface Props {
  bottomHeight: number
  topHeight: number
}
const TimeMachineVis: SFC<Props> = ({bottomHeight, topHeight}) => (
  <div style={{height: bottomHeight, fontSize: topHeight}} />
)

export default TimeMachineVis
