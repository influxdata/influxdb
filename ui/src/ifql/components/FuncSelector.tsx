import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'

import {ClickOutside} from 'src/shared/components/ClickOutside'
import FuncList from 'src/ifql/components/FuncList'

interface State {
  isOpen: boolean
  inputText: string
}

interface Props {
  funcs: string[]
  onAddNode: (name: string) => void
}

export class FuncSelector extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
      inputText: '',
    }
  }

  public render() {
    const {onAddNode} = this.props
    const {isOpen, inputText} = this.state

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div className={`dropdown dashboard-switcher ${this.openClass}`}>
          <button
            className="btn btn-square btn-default btn-sm dropdown-toggle"
            onClick={this.handleClick}
          >
            <span className="icon plus" />
          </button>
          <FuncList
            inputText={inputText}
            onAddNode={onAddNode}
            isOpen={isOpen}
            funcs={this.availableFuncs}
            onInputChange={this.handleInputChange}
            onKeyDown={this.handleKeyDown}
          />
        </div>
      </ClickOutside>
    )
  }

  private get openClass(): string {
    if (this.state.isOpen) {
      return 'open'
    }

    return ''
  }

  private get availableFuncs(): string[] {
    return this.props.funcs.filter(f =>
      f.toLowerCase().includes(this.state.inputText)
    )
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({inputText: e.target.value})
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key !== 'Escape') {
      return
    }

    this.setState({inputText: '', isOpen: false})
  }

  private handleClick = () => {
    this.setState({isOpen: !this.state.isOpen})
  }

  private handleClickOutside = () => {
    this.setState({isOpen: false})
  }
}

export default FuncSelector
