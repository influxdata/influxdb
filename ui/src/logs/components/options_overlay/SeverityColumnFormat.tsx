// Libraries
import React, {PureComponent} from 'react'

// Components
import {Radio, ButtonShape} from 'src/clockface'

// Constants
import {SeverityFormatOptions} from 'src/types/logs'

// Types
import {SeverityFormat} from 'src/types/logs'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  format: SeverityFormat
  onChangeFormat: (format: SeverityFormat) => void
}

@ErrorHandling
class SeverityColumnFormat extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {format, onChangeFormat} = this.props

    return (
      <div className="graph-options-group">
        <label className="form-label">Severity Format</label>
        <Radio shape={ButtonShape.StretchToFit}>
          <Radio.Button
            active={format === SeverityFormatOptions.Dot}
            id="severity-format-option--dot"
            value={SeverityFormatOptions.Dot}
            onClick={onChangeFormat}
            titleText="Show only a dot in the severity column"
          >
            Dot
          </Radio.Button>
          <Radio.Button
            active={format === SeverityFormatOptions.DotText}
            id="severity-format-option--dot-text"
            value={SeverityFormatOptions.DotText}
            onClick={onChangeFormat}
            titleText="Show both a dot and the severity name in the severity column"
          >
            Dot + Text
          </Radio.Button>
          <Radio.Button
            active={format === SeverityFormatOptions.Text}
            id="severity-format-option--text"
            value={SeverityFormatOptions.Text}
            onClick={onChangeFormat}
            titleText="Show only the severity name in the severity column"
          >
            Text
          </Radio.Button>
        </Radio>
      </div>
    )
  }
}

export default SeverityColumnFormat
