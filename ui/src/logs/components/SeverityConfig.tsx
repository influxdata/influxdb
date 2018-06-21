import React, {SFC} from 'react'
import uuid from 'uuid'
import ColorDropdown from 'src/logs/components/ColorDropdown'
import {SeverityLevel, SeverityColor} from 'src/types/logs'

interface Props {
  configs: SeverityLevel[]
  onReset: () => void
  onChangeSeverityLevel: (severity: string) => (override: SeverityColor) => void
}

const SeverityConfig: SFC<Props> = ({
  configs,
  onReset,
  onChangeSeverityLevel,
}) => (
  <>
    <label className="form-label">Customize Severity Colors</label>
    <div className="logs-options--color-list">
      {configs.map(config => (
        <div key={uuid.v4()} className="logs-options--color-row">
          <div className="logs-options--color-column">
            <div className="logs-options--label">{config.severity}</div>
          </div>
          <div className="logs-options--color-column">
            <ColorDropdown
              selected={config.override || config.default}
              onChoose={onChangeSeverityLevel(config.severity)}
              stretchToFit={true}
            />
          </div>
        </div>
      ))}
    </div>
    <button className="btn btn-sm btn-default btn-block" onClick={onReset}>
      <span className="icon refresh" />
      Reset to Defaults
    </button>
  </>
)

export default SeverityConfig
