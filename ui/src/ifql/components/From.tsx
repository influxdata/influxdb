import React, {PureComponent} from 'react'

import {getDatabases} from 'src/ifql/apis'

import Dropdown from 'src/shared/components/Dropdown'
import {OnChangeArg} from 'src/types/ifql'

interface Props {
  funcID: string
  argKey: string
  value: string
  bodyID: string
  declarationID: string
  onChangeArg: OnChangeArg
}

interface State {
  dbs: string[]
}

interface DropdownItem {
  text: string
}

class From extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      dbs: [],
    }
  }

  public async componentDidMount() {
    try {
      const dbs = await getDatabases()
      this.setState({dbs})
    } catch (error) {
      // TODO: notity error
    }
  }

  public render() {
    const {value, argKey} = this.props

    return (
      <div className="func-arg">
        <label className="func-arg--label">{argKey}</label>
        <Dropdown
          selected={value}
          className="from--dropdown dropdown-160 func-arg--value"
          menuClass="dropdown-astronaut"
          buttonColor="btn-default"
          items={this.items}
          onChoose={this.handleChooseDatabase}
        />
      </div>
    )
  }

  private handleChooseDatabase = (item: DropdownItem): void => {
    const {argKey, funcID, onChangeArg, bodyID, declarationID} = this.props
    onChangeArg({
      funcID,
      key: argKey,
      value: item.text,
      bodyID,
      declarationID,
      generate: true,
    })
  }

  private get items(): DropdownItem[] {
    return this.state.dbs.map(text => ({text}))
  }
}

export default From
