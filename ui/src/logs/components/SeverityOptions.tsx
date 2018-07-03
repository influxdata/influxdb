import React, {SFC} from 'react'
import uuid from 'uuid'
import ColorDropdown from 'src/logs/components/ColorDropdown'
import SeverityColumnFormat from 'src/logs/components/SeverityColumnFormat'
import {SeverityLevel, SeverityColor, SeverityFormat} from 'src/types/logs'

interface Props {
  severityLevels: SeverityLevel[]
  onReset: () => void
  onChangeSeverityLevel: (severity: string, override: SeverityColor) => void
  severityFormat: SeverityFormat
  onChangeSeverityFormat: (format: SeverityFormat) => void
}

const SeverityConfig: SFC<Props> = ({
  severityLevels,
  onReset,
  onChangeSeverityLevel,
  severityFormat,
  onChangeSeverityFormat,
}) => (
  <>
    <label className="form-label">Severity Colors</label>
    <div className="logs-options--color-list">
      {severityLevels.map(config => (
        <div key={uuid.v4()} className="logs-options--color-row">
          <div className="logs-options--color-column">
            <div className="logs-options--color-label">{config.severity}</div>
          </div>
          <div className="logs-options--color-column">
            <ColorDropdown
              selected={config.override || config.default}
              onChoose={onChangeSeverityLevel}
              stretchToFit={true}
              severityLevel={config.severity}
            />
          </div>
        </div>
      ))}
    </div>
    <button className="btn btn-sm btn-default btn-block" onClick={onReset}>
      <span className="icon refresh" />
      Reset to Defaults
    </button>
    <SeverityColumnFormat
      format={severityFormat}
      onChangeFormat={onChangeSeverityFormat}
    />
  </>
)

export default SeverityConfig
