// Libraries
import React, {PureComponent, createRef} from 'react'

// Component
import FunctionTooltip from 'src/timeMachine/components/fluxFunctionsToolbar/FunctionTooltip'

// Types
import {FluxToolbarFunction} from 'src/types/shared'

interface Props {
  func: FluxToolbarFunction
  onClickFunction: (name: string, example: string) => void
  testID?: string
}

interface State {
  isActive: boolean
  hoverPosition: {top: number; right: number}
}

class ToolbarFunction extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    testID: 'toolbar-function',
  }
  public state: State = {isActive: false, hoverPosition: undefined}
  private functionRef = createRef<HTMLDivElement>()

  public render() {
    const {func, testID} = this.props

    return (
      <div
        className="flux-functions-toolbar--function"
        ref={this.functionRef}
        onMouseEnter={this.handleHover}
        onMouseLeave={this.handleStopHover}
        data-testid={testID}
      >
        {this.tooltip}
        <dd
          onClick={this.handleClickFunction}
          data-testid={`flux-function ${func.name}`}
        >
          {func.name} {this.helperText}
        </dd>
      </div>
    )
  }

  private get tooltip(): JSX.Element | null {
    if (this.state.isActive) {
      return (
        <FunctionTooltip
          func={this.props.func}
          onDismiss={this.handleStopHover}
          tipPosition={this.state.hoverPosition}
        />
      )
    }

    return null
  }

  private get helperText(): JSX.Element | null {
    if (this.state.isActive) {
      return (
        <span className="flux-functions-toolbar--helper">Click to Add</span>
      )
    }

    return null
  }

  private handleHover = () => {
    const {top, left} = this.functionRef.current.getBoundingClientRect()
    const right = window.innerWidth - left

    this.setState({isActive: true, hoverPosition: {top, right}})
  }

  private handleStopHover = () => {
    this.setState({isActive: false})
  }

  private handleClickFunction = () => {
    const {func, onClickFunction} = this.props

    onClickFunction(func.name, func.example)
  }
}

export default ToolbarFunction
