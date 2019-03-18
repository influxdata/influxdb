// Libraries
import React, {FunctionComponent, useRef, useState} from 'react'

// Components
import VariableTooltipContents from 'src/timeMachine/components/variableToolbar/VariableTooltipContents'
import BoxTooltip from 'src/shared/components/BoxTooltip'

// Types
import {Variable} from '@influxdata/influx'

interface Props {
  variable: Variable
}

const VariableItem: FunctionComponent<Props> = ({variable}) => {
  const trigger = useRef<HTMLDivElement>(null)
  const [tooltipVisible, setTooltipVisible] = useState(false)

  let triggerRect: DOMRect = null

  if (trigger.current) {
    triggerRect = trigger.current.getBoundingClientRect() as DOMRect
  }

  return (
    <div
      className="variables-toolbar--item"
      onMouseEnter={() => setTooltipVisible(true)}
      onMouseLeave={() => setTooltipVisible(false)}
      ref={trigger}
    >
      <div className="variables-toolbar--label">{variable.name}</div>
      {tooltipVisible && (
        <BoxTooltip triggerRect={triggerRect as DOMRect}>
          <VariableTooltipContents variableID={variable.id} />
        </BoxTooltip>
      )}
    </div>
  )
}

export default VariableItem
