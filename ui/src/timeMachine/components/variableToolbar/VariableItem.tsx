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

function shouldShowTooltip(variable: Variable): boolean {
  // a system variable isn't selectable in this manor
  if (variable.arguments.type === 'system') {
    return false
  }

  // if a static list is empty or has a single component, we dont want to show the tooltip,
  // as it would have nothing for the user to do on it
  if (
    variable.arguments.type === 'constant' &&
    variable.arguments.values.length <= 1
  ) {
    return false
  }

  // if a static map object is empty or has a single component, we dont want to show the tooltip,
  // as it would have nothing for the user to do on it
  if (
    variable.arguments.type === 'map' &&
    Object.keys(variable.arguments.values.length) <= 1
  ) {
    return false
  }

  return true
}

const VariableItem: FC<Props> = ({variable, onClickVariable}) => {
  const trigger = useRef<HTMLDivElement>(null)

  if (!shouldShowTooltip(variable)) {
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
        contents={() => <VariableTooltipContents variableID={variable.id} />}
      />
    </div>
  )
}

export default VariableItem
