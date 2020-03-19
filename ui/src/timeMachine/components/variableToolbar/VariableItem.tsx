// Libraries
import React, {FC, useRef} from 'react'

// Components
import VariableTooltipContents from 'src/timeMachine/components/variableToolbar/VariableTooltipContents'
import {
  Popover,
  PopoverPosition,
  PopoverInteraction,
  Appearance,
} from '@influxdata/clockface'

// Types
import {Variable} from 'src/types'
import VariableLabel from 'src/timeMachine/components/variableToolbar/VariableLabel'

interface Props {
  variable: Variable
  onClickVariable: (variableName: string) => void
}

const VariableItem: FC<Props> = ({variable, onClickVariable}) => {
  const trigger = useRef<HTMLDivElement>(null)

  if (
      (variable.arguments.type === 'constant' && variable.arguments.values.length <= 1) ||
      (variable.arguments.type === 'map' && Object.keys(variable.arguments.values).length <= 1)
  ) {
      return (
    <div className="variables-toolbar--item" ref={trigger}>
      <VariableLabel name={variable.name} onClickVariable={onClickVariable} />
    </div>
      )
  }

  return (
    <div className="variables-toolbar--item" ref={trigger}>
      <VariableLabel name={variable.name} onClickVariable={onClickVariable} />
      <Popover
        appearance={Appearance.Outline}
        position={PopoverPosition.ToTheLeft}
        triggerRef={trigger}
        showEvent={PopoverInteraction.Hover}
        hideEvent={PopoverInteraction.Hover}
        distanceFromTrigger={8}
        testID="toolbar-popover"
        contents={() => (
          <VariableTooltipContents
            variable={variable}
            variableID={variable.id}
          />
        )}
      />
    </div>
  )
}

export default VariableItem
