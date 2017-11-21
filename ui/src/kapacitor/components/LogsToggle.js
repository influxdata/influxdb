import React, {PropTypes} from 'react'

const LogsToggle = ({areLogsVisible, areLogsEnabled, onToggleLogsVisbility}) =>
  <ul className="nav nav-tablist nav-tablist-sm nav-tablist-malachite logs-toggle">
    <li
      className={areLogsVisible ? null : 'active'}
      onClick={onToggleLogsVisbility}
    >
      Editor
    </li>
    {areLogsEnabled &&
      <li
        className={areLogsVisible ? 'active' : null}
        onClick={onToggleLogsVisbility}
      >
        Editor + Logs
      </li>}
  </ul>

const {bool, func} = PropTypes

LogsToggle.propTypes = {
  areLogsVisible: bool,
  areLogsEnabled: bool,
  onToggleLogsVisbility: func.isRequired,
}

export default LogsToggle
