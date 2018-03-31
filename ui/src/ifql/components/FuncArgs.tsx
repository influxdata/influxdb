import React, {PureComponent} from 'react'

interface Arg {
  key: string
  value: string
  type: string
}

export interface Func {
  name: string
  args: Arg[]
}

interface Props {
  func: Func
}

export default class FuncArgs extends PureComponent<Props> {
  public render() {
    return (
      <div className="func-args">
        {this.props.func.args.map(({key, value}) => (
          <div className="func-arg" key={key}>
            {key} : {value}
          </div>
        ))}
      </div>
    )
  }
}
