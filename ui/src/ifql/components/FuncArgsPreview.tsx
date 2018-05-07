import React, {PureComponent} from 'react'
import {Arg} from 'src/types/ifql'
import uuid from 'uuid'

interface Props {
  args: Arg[]
}

export default class FuncArgsPreview extends PureComponent<Props> {
  public render() {
    return <div className="func-node--preview">{this.summarizeArguments}</div>
  }

  private get summarizeArguments(): JSX.Element | JSX.Element[] {
    const {args} = this.props

    if (!args) {
      return
    }

    return this.colorizedArguments
  }

  private get colorizedArguments(): JSX.Element | JSX.Element[] {
    const {args} = this.props

    return args.map((arg, i): JSX.Element => {
      if (!arg.value) {
        return
      }

      const separator = i === 0 ? null : ', '

      return (
        <React.Fragment key={uuid.v4()}>
          {separator}
          {arg.key}: {this.colorArgType(`${arg.value}`, arg.type)}
        </React.Fragment>
      )
    })
  }

  private colorArgType = (argument: string, type: string): JSX.Element => {
    switch (type) {
      case 'time':
      case 'number':
      case 'period':
      case 'duration':
      case 'array': {
        return <span className="variable-value--number">{argument}</span>
      }
      case 'bool': {
        return <span className="variable-value--boolean">{argument}</span>
      }
      case 'string': {
        return <span className="variable-value--string">"{argument}"</span>
      }
      case 'invalid': {
        return <span className="variable-value--invalid">{argument}</span>
      }
      default: {
        return <span>{argument}</span>
      }
    }
  }
}
