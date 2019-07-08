// Libraries
import React, {PureComponent, createRef} from 'react'

// Component
import FunctionTooltipContents from 'src/timeMachine/components/fluxFunctionsToolbar/FunctionTooltipContents'
import BoxTooltip from 'src/shared/components/BoxTooltip'

// Types
import {FluxToolbarFunction} from 'src/types/shared'

interface Props {
  func: FluxToolbarFunction
  onClickFunction: (func: FluxToolbarFunction) => void
  testID: string
}

interface State {
  isActive: boolean
}

class ToolbarFunction extends PureComponent<Props, State> {
  public static defaultProps = {
    testID: 'toolbar-function',
  }

  public state: State = {isActive: false}

  private functionRef = createRef<HTMLDivElement>()

  public render() {
    const {func, testID} = this.props
    const {isActive} = this.state

    return (
      <div
        className="flux-functions-toolbar--function"
        ref={this.functionRef}
        onMouseEnter={this.handleHover}
        onMouseLeave={this.handleStopHover}
        data-testid={testID}
      >
        {isActive && (
          <BoxTooltip triggerRect={this.domRect}>
            <FunctionTooltipContents func={func} />
          </BoxTooltip>
        )}
        <dd
          onClick={this.handleClickFunction}
          data-testid={`flux-function ${func.name}`}
        >
          {func.name} {this.helperText}
        </dd>
      </div>
    )
  }

  private get domRect(): DOMRect {
    if (!this.functionRef.current) {
      return null
    }

    return this.functionRef.current.getBoundingClientRect() as DOMRect
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
    this.setState({isActive: true})
  }

  private handleStopHover = () => {
    this.setState({isActive: false})
  }

  private handleClickFunction = () => {
    const {func, onClickFunction} = this.props

    onClickFunction(func)
  }
}

export default ToolbarFunction
