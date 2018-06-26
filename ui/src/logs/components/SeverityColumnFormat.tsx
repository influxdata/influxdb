import React, {SFC} from 'react'

import {SeverityFormatOptions} from 'src/logs/constants'
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
      <li
        onClick={onChangeFormat(SeverityFormatOptions.dot)}
        className={className(SeverityFormatOptions.dot, format)}
      >
        Dot
      </li>
      <li
        onClick={onChangeFormat(SeverityFormatOptions.dotText)}
        className={className(SeverityFormatOptions.dotText, format)}
      >
        Dot + Text
      </li>
      <li
        onClick={onChangeFormat(SeverityFormatOptions.text)}
        className={className(SeverityFormatOptions.text, format)}
      >
        Text
      </li>
    </ul>
  </div>
)

export default SeverityFormat
