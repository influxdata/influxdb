import React, {SFC} from 'react'

interface Props {
  areLogsVisible: boolean
  onToggleLogsVisibility: () => void
}

const LogsToggle: SFC<Props> = ({areLogsVisible, onToggleLogsVisibility}) => (
  <ul className="nav nav-tablist nav-tablist-sm nav-tablist-malachite">
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
)

export default LogsToggle
