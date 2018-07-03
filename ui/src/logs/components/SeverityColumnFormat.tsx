import React, {PureComponent, MouseEvent} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {SeverityFormatOptions} from 'src/logs/constants'
import {SeverityFormat} from 'src/types/logs'

interface Props {
  format: SeverityFormat
  onChangeFormat: (format: SeverityFormat) => void
}

const className = (name: SeverityFormat, format: SeverityFormat): string => {
  if (name === format) {
    return 'active'
  }

  return null
}

@ErrorHandling
class SeverityColumnFormat extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {format} = this.props

    return (
      <div className="graph-options-group">
        <label className="form-label">Severity Format</label>
        <ul className="nav nav-tablist nav-tablist-sm stretch">
          <li
            data-tag-value={SeverityFormatOptions.dot}
            onClick={this.handleClick}
            className={className(SeverityFormatOptions.dot, format)}
          >
            Dot
          </li>
          <li
            data-tag-value={SeverityFormatOptions.dotText}
            onClick={this.handleClick}
            className={className(SeverityFormatOptions.dotText, format)}
          >
            Dot + Text
          </li>
          <li
            data-tag-value={SeverityFormatOptions.text}
            onClick={this.handleClick}
            className={className(SeverityFormatOptions.text, format)}
          >
            Text
          </li>
        </ul>
      </div>
    )
  }

  private handleClick = (e: MouseEvent<HTMLElement>) => {
    const {onChangeFormat} = this.props
    const target = e.target as HTMLElement
    const value = target.dataset.tagValue

    onChangeFormat(SeverityFormatOptions[value])
  }
}

export default SeverityColumnFormat
