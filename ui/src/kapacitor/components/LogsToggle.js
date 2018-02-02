import React, {PropTypes} from 'react'

const LogsToggle = ({areLogsVisible, onToggleLogsVisibility}) =>
  <ul className="nav nav-tablist nav-tablist-sm nav-tablist-malachite logs-toggle">
    <li
      className={areLogsVisible ? null : 'active'}
      onClick={onToggleLogsVisibility}
    >
      Editor
    </li>
    <li
      className={areLogsVisible ? 'active' : null}
      onClick={onToggleLogsVisibility}
    >
      Editor + Logs
    </li>
  </ul>

const {bool, func} = PropTypes

LogsToggle.propTypes = {
  areLogsVisible: bool,
  onToggleLogsVisibility: func.isRequired,
}

export default LogsToggle
