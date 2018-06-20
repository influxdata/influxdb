import React, {SFC} from 'react'
import uuid from 'uuid'
import ColorDropdown, {Color} from 'src/logs/components/ColorDropdown'

interface SeverityItem {
  severity: string
  default: Color
  override?: Color
}

interface Props {
  configs: SeverityItem[]
  onReset: () => void
  onChangeColor: (severity: string) => (override: Color) => void
}

const SeverityConfig: SFC<Props> = ({configs, onReset, onChangeColor}) => (
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
              onChoose={onChangeColor(config.severity)}
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
