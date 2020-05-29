// Libraries
import React, {FC, useRef} from 'react'
import {get} from 'lodash'

// Components
import VariableTooltipContents from 'src/timeMachine/components/variableToolbar/VariableTooltipContents'
import {
  Button,
  Popover,
  PopoverPosition,
  PopoverInteraction,
  Appearance,
  ComponentColor,
  ComponentSize,
} from '@influxdata/clockface'

// Types
import {Variable} from 'src/types'

interface Props {
  variable: Variable
  onClickVariable: (variableName: string) => void
  testID?: string
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
    Object.keys(variable.arguments.values).length <= 1
  ) {
    return false
  }

  if (
    variable.arguments.type === 'query' &&
    (!(variable.arguments.values as any).results ||
      (variable.arguments.values as any).results.length <= 1)
  ) {
    return false
  }

  return true
}

const VariableItem: FC<Props> = ({
  variable,
  onClickVariable,
  testID = 'variable',
}) => {
  const trigger = useRef<HTMLDivElement>(null)

  const handleClick = (): void => {
    const variableName = get(variable, 'name', 'variableName')
    onClickVariable(variableName)
  }

  if (!shouldShowTooltip(variable)) {
    return (
      <div
        className="flux-toolbar--list-item flux-toolbar--variable"
        data-testid={`variable--${testID}`}
      >
        <code data-testid={`variable-name--${testID}`}>{variable.name}</code>
        <Button
          testID={`variable--${testID}--inject`}
          text="Inject"
          onClick={handleClick}
          size={ComponentSize.ExtraSmall}
          className="flux-toolbar--injector"
          color={ComponentColor.Success}
        />
      </div>
    )
  }

  return (
    <>
      <div
        className="flux-toolbar--list-item flux-toolbar--variable"
        ref={trigger}
        data-testid={`variable--${testID}`}
      >
        <code data-testid={`variable-name--${testID}`}>{variable.name}</code>
        <Button
          testID={`variable--${testID}--inject`}
          text="Inject"
          onClick={handleClick}
          size={ComponentSize.ExtraSmall}
          className="flux-toolbar--injector"
          color={ComponentColor.Success}
        />
      </div>
      <Popover
        appearance={Appearance.Outline}
        position={PopoverPosition.ToTheLeft}
        triggerRef={trigger}
        showEvent={PopoverInteraction.Hover}
        hideEvent={PopoverInteraction.Hover}
        color={ComponentColor.Success}
        distanceFromTrigger={8}
        testID="toolbar-popover"
        enableDefaultStyles={false}
        contents={() => <VariableTooltipContents variableID={variable.id} />}
      />
    </>
  )
}

export default VariableItem
