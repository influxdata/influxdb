import React, {PureComponent} from 'react'
import uuid from 'uuid'
import _ from 'lodash'

import {Func} from 'src/types/flux'
import {funcNames} from 'src/flux/constants'
import FilterPreview from 'src/flux/components/FilterPreview'

import {getDeep} from 'src/utils/wrappers'

interface Props {
  func: Func
}

export default class FuncArgsPreview extends PureComponent<Props> {
  public render() {
    return <div className="func-node--preview">{this.summarizeArguments}</div>
  }

  private get summarizeArguments(): JSX.Element | JSX.Element[] {
    const {func} = this.props
    const {args} = func

    if (!args) {
      return
    }

    if (func.name === funcNames.FILTER) {
      const value = getDeep<string>(args, '0.value', '')
      if (!value) {
        return this.colorizedArguments
      }

      return <FilterPreview filterString={value} />
    }

    return this.colorizedArguments
  }

  private get colorizedArguments(): JSX.Element | JSX.Element[] {
    const {func} = this.props
    const {args} = func

    return args.map((arg, i): JSX.Element => {
      if (!arg.value) {
        return
      }

      const separator = i === 0 ? null : ', '
      let argValue
      if (arg.type === 'object') {
        const valueMap = _.map(arg.value, (value, key) => `${key}:${value}`)
        argValue = `{${valueMap.join(', ')}}`
      } else {
        argValue = `${arg.value}`
      }

      return (
        <React.Fragment key={uuid.v4()}>
          {separator}
          {arg.key}: {this.colorArgType(argValue, arg.type)}
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
        return <span className="func-arg--number">{argument}</span>
      }
      case 'bool': {
        return <span className="func-arg--boolean">{argument}</span>
      }
      case 'string': {
        return <span className="func-arg--string">"{argument}"</span>
      }
      case 'object': {
        return <span className="func-arg--object">{argument}</span>
      }
      case 'invalid': {
        return <span className="func-arg--invalid">{argument}</span>
      }
      default: {
        return <span>{argument}</span>
      }
    }
  }
}
