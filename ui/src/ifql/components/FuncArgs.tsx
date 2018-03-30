import React, {PureComponent} from 'react'
import _ from 'lodash'

export interface Params {
  [key: string]: string
}

export interface Func {
  name: string
  params: Params
}

interface Arg {
  key: string
  value: string
}

interface Props {
  func: Func
  args: Arg[]
}

export default class FuncArgs extends PureComponent<Props> {
  public render() {
    return (
      <div>
        {this.paramNames.map(name => (
          <div key={name}>
            {name} : {this.getArgumentValue(name)}
          </div>
        ))}
      </div>
    )
  }

  private get paramNames(): string[] {
    const {func: {params}} = this.props
    return Object.keys(params)
  }

  private getArgumentValue(name: string): string {
    const {args} = this.props
    return _.get(args.find(arg => arg.key === name), 'value', '')
  }
}
