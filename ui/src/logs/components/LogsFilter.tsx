import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'
import classnames from 'classnames'
import {Filter} from 'src/types/logs'
import {getDeep} from 'src/utils/wrappers'
import {ClickOutside} from 'src/shared/components/ClickOutside'

interface Props {
  filter: Filter
  onDelete: (id: string) => void
  onChangeOperator: (id: string, newOperator: string) => void
  onChangeValue: (id: string, newValue: string) => void
}

interface State {
  editing: boolean
}

class LogsFilter extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      editing: false,
    }
  }

  public render() {
    const {
      filter: {id},
      onDelete,
    } = this.props
    const {editing} = this.state

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <li className={this.className} onClick={this.handleStartEdit}>
          {editing ? this.renderEditor : this.label}
          <div
            className="logs-viewer--filter-remove"
            onClick={this.handleDelete}
          />
        </li>
      </ClickOutside>
    )
  }

  private handleClickOutside = (): void => {
    this.setState({editing: false})
  }

  private handleStartEdit = (): void => {
    this.setState({editing: true})
  }

  private get className(): string {
    const {editing} = this.state
    return classnames('logs-viewer--filter', {active: editing})
  }

  private handleDelete = () => {
    const id = getDeep(this.props, 'filter.id', '')
    this.props.onDelete(id)
  }

  private get label(): JSX.Element {
    const {
      filter: {key, operator, value},
    } = this.props

    let displayKey = key
    if (key === 'severity_1') {
      displayKey = 'severity'
    }

    return <span>{`${displayKey} ${operator} ${value}`}</span>
  }

  private get renderEditor(): JSX.Element {
    const {
      filter: {key, operator, value},
    } = this.props

    return (
      <>
        <div>{key}</div>
        <input
          type="text"
          maxLength={2}
          value={operator}
          className="form-control monotype input-xs logs-viewer--operator"
          spellCheck={false}
          onChange={this.handleOperatorInput}
          onKeyDown={this.handleKeyDown}
        />
        <input
          type="text"
          value={value}
          className="form-control monotype input-xs logs-viewer--value"
          spellCheck={false}
          autoFocus={true}
          onChange={this.handleValueInput}
          onKeyDown={this.handleKeyDown}
        />
      </>
    )
  }

  private handleOperatorInput = (e: ChangeEvent<HTMLInputElement>): void => {
    const {
      filter: {id},
      onChangeOperator,
    } = this.props

    const cleanValue = this.enforceOperatorChars(e.target.value)

    onChangeOperator(id, cleanValue)
  }

  private handleValueInput = (e: ChangeEvent<HTMLInputElement>): void => {
    const {
      filter: {id},
      onChangeValue,
    } = this.props

    onChangeValue(id, e.target.value)
  }

  private enforceOperatorChars = text => {
    return text
      .split('')
      .filter(t => ['!', '~', `=`].includes(t))
      .join('')
  }

  private handleKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Enter') {
      e.preventDefault()
      this.setState({editing: false})
    }
  }

  private handleToggleOperator = () => {
    const id = getDeep(this.props, 'filter.id', '')

    let nextOperator = '=='
    if (this.operator === '==') {
      nextOperator = '!='
    }

    this.props.onChangeOperator(id, nextOperator)
  }

  private get toggleOperatorText(): string {
    return this.operator === '==' ? '!=' : '=='
  }

  private get operator(): string {
    return getDeep(this.props, 'filter.operator', '')
  }
}

export default LogsFilter
