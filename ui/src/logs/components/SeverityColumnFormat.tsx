import React, {SFC} from 'react'
import {SeverityFormat} from 'src/types/logs'

interface Props {
  format: SeverityFormat
  onChangeFormat: (format: SeverityFormat) => () => void
}

const className = (name: SeverityFormat, format: SeverityFormat): string => {
  if (name === format) {
    return 'active'
  }

  return null
}

const SeverityFormat: SFC<Props> = ({format, onChangeFormat}) => (
  <div className="graph-options-group">
    <label className="form-label">Severity Format</label>
    <ul className="nav nav-tablist nav-tablist-sm stretch">
      <li onClick={onChangeFormat('dot')} className={className('dot', format)}>
        Dot
      </li>
      <li
        onClick={onChangeFormat('dotText')}
        className={className('dotText', format)}
      >
        Dot + Text
      </li>
      <li
        onClick={onChangeFormat('text')}
        className={className('text', format)}
      >
        Text
      </li>
    </ul>
  </div>
)

export default SeverityFormat
