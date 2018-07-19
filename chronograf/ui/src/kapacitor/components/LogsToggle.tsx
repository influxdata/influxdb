import React, {PureComponent} from 'react'

interface Props {
  areLogsVisible: boolean
  areLogsEnabled: boolean
  onToggleLogsVisibility: () => void
}

class LogsToggle extends PureComponent<Props> {
  public render() {
    return (
      <ul className="nav nav-tablist nav-tablist-sm nav-tablist-malachite">
        {this.leftTab}
        {this.rightTab}
      </ul>
    )
  }

  private get leftTab(): JSX.Element {
    const {areLogsEnabled, areLogsVisible, onToggleLogsVisibility} = this.props

    if (areLogsEnabled) {
      return (
        <li
          className={areLogsVisible ? null : 'active'}
          onClick={onToggleLogsVisibility}
        >
          Editor
        </li>
      )
    }

    return (
      <li
        className={areLogsVisible ? 'disabled' : ' disabled active'}
        title="Log viewing is currently disabled"
      >
        Editor
      </li>
    )
  }

  private get rightTab(): JSX.Element {
    const {areLogsEnabled, areLogsVisible, onToggleLogsVisibility} = this.props

    if (areLogsEnabled) {
      return (
        <li
          className={areLogsVisible ? 'active' : null}
          onClick={onToggleLogsVisibility}
        >
          Editor + Logs
        </li>
      )
    }

    return (
      <li
        className={areLogsVisible ? 'disabled active' : ' disabled'}
        title="Log viewing is currently disabled"
      >
        Editor + Logs
      </li>
    )
  }
}

export default LogsToggle
