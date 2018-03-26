import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import DropdownInput from 'src/shared/components/DropdownInput'

import {ClickOutside} from 'src/shared/components/ClickOutside'

interface State {
  isOpen: boolean
  inputText: string
}

interface Props {
  funcs: string[]
}

export class FuncsButton extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
      inputText: '',
    }
  }

  public render() {
    const {isOpen, inputText} = this.state

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div className={`dropdown dashboard-switcher ${isOpen ? 'open' : ''}`}>
          <button
            className="btn btn-square btn-default btn-sm dropdown-toggle"
            onClick={this.handleClick}
          >
            <span className="icon plus" />
          </button>
          <ul className="dropdown-menu funcs">
            <DropdownInput
              buttonSize="btn-xs"
              buttonColor="btn-default"
              onFilterChange={this.handleInputChange}
              onFilterKeyPress={this.handleKeyDown}
              searchTerm={inputText}
            />
            <FancyScrollbar autoHide={false} autoHeight={true} maxHeight={240}>
              {isOpen &&
                this.availableFuncs.map((func, i) => (
                  <li className="dropdown-item func" key={i}>
                    <a>{func}</a>
                  </li>
                ))}
            </FancyScrollbar>
          </ul>
        </div>
      </ClickOutside>
    )
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

export default FuncsButton
