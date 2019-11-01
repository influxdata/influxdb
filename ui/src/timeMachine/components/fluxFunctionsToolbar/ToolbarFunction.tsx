// Libraries
import React, {FC, createRef} from 'react'

// Component
import FunctionTooltipContents from 'src/timeMachine/components/fluxFunctionsToolbar/FunctionTooltipContents'
import {
  Popover,
  PopoverPosition,
  PopoverInteraction,
  PopoverType,
} from '@influxdata/clockface'

// Types
import {FluxToolbarFunction} from 'src/types/shared'

interface Props {
  func: FluxToolbarFunction
  onClickFunction: (func: FluxToolbarFunction) => void
  testID: string
}

const defaultProps = {
  testID: 'toolbar-function',
}

const ToolbarFunction: FC<Props> = ({func, onClickFunction, testID}) => {
  const functionRef = createRef<HTMLDivElement>()
  const handleClickFunction = () => {
    onClickFunction(func)
  }
  return (
    <div
      className="flux-functions-toolbar--function"
      ref={functionRef}
      data-testid={testID}
    >
      <Popover
        type={PopoverType.Outline}
        position={PopoverPosition.ToTheLeft}
        triggerRef={functionRef}
        showEvent={PopoverInteraction.Hover}
        hideEvent={PopoverInteraction.Hover}
        distanceFromTrigger={8}
        testID="toolbar-popover"
        contents={() => <FunctionTooltipContents func={func} />}
      />
      <dd
        onClick={handleClickFunction}
        data-testid={`flux-function ${func.name}`}
      >
        {func.name}
        &nbsp;
        <span className="flux-functions-toolbar--helper">Click to Add</span>
      </dd>
    </div>
  )
}

ToolbarFunction.defaultProps = defaultProps

export default ToolbarFunction
