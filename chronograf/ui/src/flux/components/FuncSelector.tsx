import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'
import _ from 'lodash'
import classnames from 'classnames'

import {ClickOutside} from 'src/shared/components/ClickOutside'
import FuncList from 'src/flux/components/FuncList'
import {OnAddNode} from 'src/types/flux'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface State {
  isOpen: boolean
  inputText: string
  selectedFunc: string
}

interface Props {
  funcs: string[]
  bodyID: string
  declarationID: string
  onAddNode: OnAddNode
  connectorVisible?: boolean
}

@ErrorHandling
export class FuncSelector extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    connectorVisible: true,
  }

  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
      inputText: '',
      selectedFunc: '',
    }
  }

  public render() {
    const {isOpen, inputText, selectedFunc} = this.state
    const {connectorVisible} = this.props

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div className={this.className}>
          {connectorVisible && <div className="func-selector--connector" />}
          {isOpen ? (
            <FuncList
              inputText={inputText}
              onAddNode={this.handleAddNode}
              funcs={this.availableFuncs}
              onInputChange={this.handleInputChange}
              onKeyDown={this.handleKeyDown}
              selectedFunc={selectedFunc}
              onSetSelectedFunc={this.handleSetSelectedFunc}
            />
          ) : (
            <button
              className="btn btn-square btn-primary btn-sm flux-func--button"
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

  private get className(): string {
    const {isOpen} = this.state

    return classnames('flux-func--selector', {open: isOpen})
  }

  private handleCloseList = () => {
    this.setState({isOpen: false, selectedFunc: ''})
  }

  private handleAddNode = (name: string) => {
    const {bodyID, declarationID} = this.props
    this.handleCloseList()
    this.props.onAddNode(name, bodyID, declarationID)
  }

  private get availableFuncs() {
    return this.props.funcs.filter(f =>
      f.toLowerCase().includes(this.state.inputText)
    )
  }

  private setSelectedFunc = () => {
    const {selectedFunc} = this.state

    const isSelectedVisible = !!this.availableFuncs.find(
      a => a === selectedFunc
    )
    const newSelectedFunc =
      this.availableFuncs.length > 0 ? this.availableFuncs[0] : ''

    this.setState({
      selectedFunc: isSelectedVisible ? selectedFunc : newSelectedFunc,
    })
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState(
      {
        inputText: e.target.value,
      },
      this.setSelectedFunc
    )
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    const {selectedFunc} = this.state
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
        this.availableFuncs,
        func => func === selectedFunc
      )
      const previousIndex = selectedIndex - 1
      // if there is selectedIndex - 1 in availableFuncs make that the new SelectedFunc
      if (previousIndex >= 0) {
        return this.setState({selectedFunc: this.availableFuncs[previousIndex]})
      }
      // if not then keep selectedFunc as is
    }

    if (e.key === 'ArrowDown' && selectedFuncExists) {
      // get index of selectedFunc in availableFuncs
      const selectedIndex = _.findIndex(
        this.availableFuncs,
        func => func === selectedFunc
      )
      const nextIndex = selectedIndex + 1
      // if there is selectedIndex + 1 in availableFuncs make that the new SelectedFunc
      if (nextIndex < this.availableFuncs.length) {
        return this.setState({selectedFunc: this.availableFuncs[nextIndex]})
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
      selectedFunc: funcs[0],
    })
  }

  private handleClickOutside = () => {
    this.handleCloseList()
  }
}

export default FuncSelector
