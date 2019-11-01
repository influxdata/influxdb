// Libraries
import React, {FC, useRef} from 'react'

// Components
import VariableTooltipContents from 'src/timeMachine/components/variableToolbar/VariableTooltipContents'
import {
  Popover,
  PopoverPosition,
  PopoverInteraction,
  PopoverType,
} from '@influxdata/clockface'

// Types
import {IVariable as Variable} from '@influxdata/influx'
import VariableLabel from 'src/timeMachine/components/variableToolbar/VariableLabel'

interface Props {
  variable: Variable
  onClickVariable: (variableName: string) => void
}

const VariableItem: FC<Props> = ({variable, onClickVariable}) => {
  const trigger = useRef<HTMLDivElement>(null)

  return (
    <div className="variables-toolbar--item" ref={trigger}>
      <VariableLabel name={variable.name} onClickVariable={onClickVariable} />
      <Popover
        type={PopoverType.Outline}
        position={PopoverPosition.ToTheLeft}
        triggerRef={trigger}
        showEvent={PopoverInteraction.Hover}
        hideEvent={PopoverInteraction.Hover}
        distanceFromTrigger={8}
        testID="toolbar-popover"
        contents={() => <VariableTooltipContents variableID={variable.id} />}
      />
    </div>
  )
}

export default VariableItem
