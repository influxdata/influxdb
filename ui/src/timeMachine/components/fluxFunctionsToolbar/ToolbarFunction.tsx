// Libraries
import React, {FC, createRef} from 'react'

// Component
import FunctionTooltipContents from 'src/timeMachine/components/fluxFunctionsToolbar/FunctionTooltipContents'
import {
  Popover,
  PopoverPosition,
  PopoverInteraction,
  Appearance,
  Button,
  ComponentSize,
  ComponentColor,
} from '@influxdata/clockface'

// Types
import {FluxToolbarFunction} from 'src/types/shared'

interface Props {
  func: FluxToolbarFunction
  onClickFunction: (func: FluxToolbarFunction) => void
  testID: string
}

const defaultProps = {
  testID: 'flux-function',
}

const ToolbarFunction: FC<Props> = ({func, onClickFunction, testID}) => {
  const functionRef = createRef<HTMLDListElement>()
  const handleClickFunction = () => {
    onClickFunction(func)
  }
  return (
    <>
      <Popover
        appearance={Appearance.Outline}
        enableDefaultStyles={false}
        position={PopoverPosition.ToTheLeft}
        triggerRef={functionRef}
        showEvent={PopoverInteraction.Hover}
        hideEvent={PopoverInteraction.Hover}
        distanceFromTrigger={8}
        testID="toolbar-popover"
        contents={() => <FunctionTooltipContents func={func} />}
      />
      <dd
        ref={functionRef}
        data-testid={`flux--${testID}`}
        className="flux-toolbar--list-item flux-toolbar--function"
      >
        <code>{func.name}</code>
        <Button
          testID={`flux--${testID}--inject`}
          text="Inject"
          onClick={handleClickFunction}
          size={ComponentSize.ExtraSmall}
          className="flux-toolbar--injector"
          color={ComponentColor.Primary}
        />
      </dd>
    </>
  )
}

ToolbarFunction.defaultProps = defaultProps

export default ToolbarFunction
