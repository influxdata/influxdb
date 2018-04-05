import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'
import _ from 'lodash'

import {ClickOutside} from 'src/shared/components/ClickOutside'
import FuncList from 'src/ifql/components/FuncList'

interface State {
  isOpen: boolean
  inputText: string
  selectedFunc: string
  availableFuncs: string[]
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
      selectedFunc: '',
      availableFuncs: [],
    }
  }

  public render() {
    const {isOpen, inputText, selectedFunc, availableFuncs} = this.state

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div className="ifql-func--selector">
          {isOpen ? (
            <FuncList
              inputText={inputText}
              onAddNode={this.handleAddNode}
              funcs={availableFuncs}
              onInputChange={this.handleInputChange}
              onKeyDown={this.handleKeyDown}
              selectedFunc={selectedFunc}
              onSetSelectedFunc={this.handleSetSelectedFunc}
            />
          ) : (
            <button
              className="btn btn-square btn-primary btn-sm ifql-func--button"
              onClick={this.handleOpenList}
              tabIndex={0}
            >
              <span className="icon plus" />
            </button>
          )}
        </div>
      </ClickOutside>
    )
  }

  private handleCloseList = () => {
    this.setState({isOpen: false, selectedFunc: '', availableFuncs: []})
  }

  private handleAddNode = (name: string) => {
    this.handleCloseList()
    this.props.onAddNode(name)
  }

  private setAvailableFuncs = inputText => {
    const {funcs} = this.props
    const {selectedFunc} = this.state

    const availableFuncs = funcs.filter(f =>
      f.toLowerCase().includes(inputText)
    )
    const isSelectedVisible = !!availableFuncs.find(a => a === selectedFunc)
    const newSelectedFunc = availableFuncs.length > 0 ? availableFuncs[0] : ''

    this.setState({
      inputText,
      availableFuncs,
      selectedFunc: isSelectedVisible ? selectedFunc : newSelectedFunc,
    })
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setAvailableFuncs(e.target.value)
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    const {selectedFunc, availableFuncs} = this.state
    const selectedFuncExists = selectedFunc !== ''

    if (e.key === 'Enter' && selectedFuncExists) {
      return this.handleAddNode(selectedFunc)
    }

    if (e.key === 'Escape' || e.key === 'Tab') {
      return this.handleCloseList()
    }

    if (e.key === 'ArrowUp' && selectedFuncExists) {
      // get index of selectedFunc in availableFuncs
      const selectedIndex = _.findIndex(
        availableFuncs,
        func => func === selectedFunc
      )
      const previousIndex = selectedIndex - 1
      // if there is selectedIndex - 1 in availableFuncs make that the new SelectedFunc
      if (previousIndex >= 0) {
        return this.setState({selectedFunc: availableFuncs[previousIndex]})
      }
      // if not then keep selectedFunc as is
    }

    if (e.key === 'ArrowDown' && selectedFuncExists) {
      // get index of selectedFunc in availableFuncs
      const selectedIndex = _.findIndex(
        availableFuncs,
        func => func === selectedFunc
      )
      const nextIndex = selectedIndex + 1
      // if there is selectedIndex + 1 in availableFuncs make that the new SelectedFunc
      if (nextIndex < availableFuncs.length) {
        return this.setState({selectedFunc: availableFuncs[nextIndex]})
      }
      // if not then keep selectedFunc as is
    }
  }

  private handleSetSelectedFunc = funcName => {
    this.setState({selectedFunc: funcName})
  }

  private handleOpenList = () => {
    const {funcs} = this.props
    this.setState({
      isOpen: true,
      inputText: '',
      availableFuncs: funcs,
      selectedFunc: funcs[0],
    })
  }

  private handleClickOutside = () => {
    this.handleCloseList()
  }
}

export default FuncSelector
