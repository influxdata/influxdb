// Libraries
import React, {FunctionComponent, useRef, useState} from 'react'

// Components
import VariableTooltipContents from 'src/timeMachine/components/variableToolbar/VariableTooltipContents'
import BoxTooltip from 'src/shared/components/BoxTooltip'

// Types
import {IVariable as Variable} from '@influxdata/influx'
import VariableLabel from 'src/timeMachine/components/variableToolbar/VariableLabel'

interface Props {
  variable: Variable
  onClickVariable: (variableName: string) => void
}

const VariableItem: FunctionComponent<Props> = ({
  variable,
  onClickVariable,
}) => {
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
      <VariableLabel name={variable.name} onClickVariable={onClickVariable} />
      {tooltipVisible && (
        <BoxTooltip triggerRect={triggerRect as DOMRect}>
          <VariableTooltipContents variableID={variable.id} />
        </BoxTooltip>
      )}
    </div>
  )
}

export default VariableItem
