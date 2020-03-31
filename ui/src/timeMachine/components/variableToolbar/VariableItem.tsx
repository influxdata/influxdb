// Libraries
import React, {FC, useRef} from 'react'

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

const VariableItem: FC<Props> = ({
  variable,
  onClickVariable,
  testID = 'variable',
}) => {
  const trigger = useRef<HTMLDivElement>(null)

  const handleClick = (): void => {
    onClickVariable(name)
  }

  return (
    <>
      <div
        className="flux-toolbar--list-item flux-toolbar--variable"
        onClick={handleClick}
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
        contents={() => (
          <VariableTooltipContents
            variable={variable}
            variableID={variable.id}
          />
        )}
      />
    </>
  )
}

export default VariableItem
