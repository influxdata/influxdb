import React, {PureComponent} from 'react'
import classnames from 'classnames'
import {Filter} from 'src/logs/containers/LogsPage'

interface Props {
  filter: Filter
  onDelete: (id: string) => () => void
  onToggleStatus: (id: string) => () => void
  onToggleOperator: (id: string) => () => void
}

interface State {
  expanded: boolean
}

class LogsFilter extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      expanded: false,
    }
  }

  public render() {
    const {
      filter: {id},
      onDelete,
    } = this.props
    const {expanded} = this.state

    return (
      <li className={this.className} onMouseLeave={this.handleMouseLeave}>
        {this.label}
        <div className="logs-viewer--filter-remove" onClick={onDelete(id)} />
        {expanded && this.renderTooltip}
      </li>
    )
  }

  private get label(): JSX.Element {
    const {
      filter: {key, operator, value},
    } = this.props

    return (
      <span
        onMouseEnter={this.handleMouseEnter}
      >{`${key} ${operator} ${value}`}</span>
    )
  }

  private get className(): string {
    const {expanded} = this.state
    const {
      filter: {enabled},
    } = this.props

    return classnames('logs-viewer--filter', {
      active: expanded,
      disabled: !enabled,
    })
  }

  private handleMouseEnter = (): void => {
    this.setState({expanded: true})
  }

  private handleMouseLeave = (): void => {
    this.setState({expanded: false})
  }

  private get renderTooltip(): JSX.Element {
    const {
      filter: {id, enabled, operator},
      onDelete,
      onToggleStatus,
      onToggleOperator,
    } = this.props

    const toggleStatusText = enabled ? 'Disable' : 'Enable'
    const toggleOperatorText = operator === '==' ? '!=' : '=='

    return (
      <ul className="logs-viewer--filter-tooltip">
        <li onClick={onToggleStatus(id)}>{toggleStatusText}</li>
        <li onClick={onToggleOperator(id)}>{toggleOperatorText}</li>
        <li onClick={onDelete(id)}>Delete</li>
      </ul>
    )
  }
}

export default LogsFilter
