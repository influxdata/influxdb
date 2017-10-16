import React, {PropTypes} from 'react'

const LogsToggle = ({areLogsVisible, onToggleLogsVisbility}) =>
  <ul className="nav nav-tablist nav-tablist-sm nav-tablist-malachite">
    <li
      className={areLogsVisible ? null : 'active'}
      onClick={onToggleLogsVisbility}
    >
      Editor
    </li>
    <li
      className={areLogsVisible ? 'active' : null}
      onClick={onToggleLogsVisbility}
    >
      Editor + Logs
    </li>
  </ul>

const {bool, func} = PropTypes

LogsToggle.propTypes = {
  areLogsVisible: bool,
  onToggleLogsVisbility: func.isRequired,
}

export default LogsToggle
