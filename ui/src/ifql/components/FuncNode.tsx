import React, {PureComponent, MouseEvent} from 'react'
import uuid from 'uuid'
import FuncArgs from 'src/ifql/components/FuncArgs'
import {OnDeleteFuncNode, OnChangeArg, Func} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  func: Func
  bodyID: string
  declarationID?: string
  onDelete: OnDeleteFuncNode
  onChangeArg: OnChangeArg
  onGenerateScript: () => void
}

interface State {
  isExpanded: boolean
}

@ErrorHandling
export default class FuncNode extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    declarationID: '',
  }

  constructor(props) {
    super(props)
    this.state = {
      isExpanded: false,
    }
  }

  public render() {
    const {
      func,
      bodyID,
      onChangeArg,
      declarationID,
      onGenerateScript,
    } = this.props
    const {isExpanded} = this.state

    return (
      <div
        className="func-node"
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
      >
        <div className="func-node--name">{func.name}</div>
        {this.coloredSyntaxArgs}
        {isExpanded && (
          <FuncArgs
            func={func}
            bodyID={bodyID}
            onChangeArg={onChangeArg}
            declarationID={declarationID}
            onGenerateScript={onGenerateScript}
            onDeleteFunc={this.handleDelete}
          />
        )}
      </div>
    )
  }

  private get coloredSyntaxArgs(): JSX.Element {
    const {
      func: {args},
    } = this.props

    if (!args) {
      return
    }

    const coloredSyntax = args.map((arg, i): JSX.Element => {
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

    return <div className="func-node--preview">{coloredSyntax}</div>
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

  private handleDelete = (): void => {
    const {func, bodyID, declarationID} = this.props

    this.props.onDelete({funcID: func.id, bodyID, declarationID})
  }

  private handleMouseEnter = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()

    this.setState({isExpanded: true})
  }

  private handleMouseLeave = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()

    this.setState({isExpanded: false})
  }
}
