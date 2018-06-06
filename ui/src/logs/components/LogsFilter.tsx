import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'
import classnames from 'classnames'
import {Filter} from 'src/logs/containers/LogsPage'
import {ClickOutside} from 'src/shared/components/ClickOutside'

interface Props {
  filter: Filter
  onDelete: (id: string) => () => void
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
          <div className="logs-viewer--filter-remove" onClick={onDelete(id)} />
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

  private get label(): JSX.Element {
    const {
      filter: {key, operator, value},
    } = this.props

    return <span>{`${key} ${operator} ${value}`}</span>
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
}

export default LogsFilter
