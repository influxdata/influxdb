import React, {PureComponent} from 'react'
import _ from 'lodash'

import Dropdown from 'src/shared/components/Dropdown'
import FuncArgInput from 'src/flux/components/FuncArgInput'
import FuncArgTextArea from 'src/flux/components/FuncArgTextArea'
import {getDeep} from 'src/utils/wrappers'

import {OnChangeArg, Func, Arg} from 'src/types/flux'
import {argTypes} from 'src/flux/constants'

interface Props {
  func: Func
  bodyID: string
  declarationID: string
  onChangeArg: OnChangeArg
  declarationsFromBody: string[]
  onGenerateScript: () => void
}

interface DropdownItem {
  text: string
}

class JoinArgs extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {
      func,
      bodyID,
      onChangeArg,
      declarationID,
      onGenerateScript,
    } = this.props
    return (
      <>
        <div className="func-arg">
          <label className="func-arg--label">tables</label>
          <Dropdown
            selected={this.table1Value}
            className="from--dropdown dropdown-100 func-arg--value"
            menuClass="dropdown-astronaut"
            buttonColor="btn-default"
            items={this.items}
            onChoose={this.handleChooseTable1}
          />
          <Dropdown
            selected={this.table2Value}
            className="from--dropdown dropdown-100 func-arg--value"
            menuClass="dropdown-astronaut"
            buttonColor="btn-default"
            items={this.items}
            onChoose={this.handleChooseTable2}
          />
        </div>
        <div className="func-arg">
          <FuncArgInput
            value={this.onValue}
            argKey={'on'}
            bodyID={bodyID}
            funcID={func.id}
            type={argTypes.STRING}
            onChangeArg={onChangeArg}
            declarationID={declarationID}
            onGenerateScript={onGenerateScript}
          />
        </div>
        <div className="func-arg">
          <FuncArgTextArea
            type={argTypes.FUNCTION}
            value={this.fnValue}
            argKey={'fn'}
            funcID={func.id}
            bodyID={bodyID}
            onChangeArg={onChangeArg}
            declarationID={declarationID}
            onGenerateScript={onGenerateScript}
          />
        </div>
      </>
    )
  }

  private handleChooseTable1 = (item: DropdownItem): void => {
    this.handleChooseTables(item.text, this.table2Value)
  }

  private handleChooseTable2 = (item: DropdownItem): void => {
    this.handleChooseTables(this.table1Value, item.text)
  }

  private handleChooseTables = (table1: string, table2: string): void => {
    const {
      onChangeArg,
      bodyID,
      declarationID,
      func,
      onGenerateScript,
    } = this.props

    onChangeArg({
      funcID: func.id,
      bodyID,
      declarationID,
      key: 'tables',
      value: {[table1]: table1, [table2]: table2},
      generate: true,
    })
    onGenerateScript()
  }

  private get items(): DropdownItem[] {
    return this.props.declarationsFromBody.map(d => ({text: d}))
  }

  private get argsArray(): Arg[] {
    const {func} = this.props
    return getDeep<Arg[]>(func, 'args', [])
  }

  private get onValue(): string {
    const onObject = this.argsArray.find(a => a.key === 'on')
    return onObject.value.toString()
  }

  private get fnValue(): string {
    const fnObject = this.argsArray.find(a => a.key === 'fn')
    return fnObject.value.toString()
  }

  private get table1Value(): string {
    const tables = this.argsArray.find(a => a.key === 'tables')
    if (tables) {
      const keys = _.keys(tables.value)
      return getDeep<string>(keys, '0', '')
    }
    return ''
  }

  private get table2Value(): string {
    const tables = this.argsArray.find(a => a.key === 'tables')

    if (tables) {
      const keys = _.keys(tables.value)
      return getDeep<string>(keys, '1', getDeep<string>(keys, '0', ''))
    }
    return ''
  }
}

export default JoinArgs
