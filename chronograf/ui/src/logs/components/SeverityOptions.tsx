import React, {SFC} from 'react'
import uuid from 'uuid'
import ColorDropdown from 'src/logs/components/ColorDropdown'
import SeverityColumnFormat from 'src/logs/components/SeverityColumnFormat'

import {SeverityColorValues} from 'src/logs/constants'

import {SeverityLevelColor, SeverityColor, SeverityFormat} from 'src/types/logs'

interface Props {
  severityLevelColors: SeverityLevelColor[]
  onReset: () => void
  onChangeSeverityLevel: (severity: string, override: SeverityColor) => void
  severityFormat: SeverityFormat
  onChangeSeverityFormat: (format: SeverityFormat) => void
}

const SeverityConfig: SFC<Props> = ({
  severityLevelColors,
  onReset,
  onChangeSeverityLevel,
  severityFormat,
  onChangeSeverityFormat,
}) => (
  <>
    <label className="form-label">Severity Colors</label>
    <div className="logs-options--color-list">
      {severityLevelColors.map(lc => {
        const color = {name: lc.color, hex: SeverityColorValues[lc.color]}
        return (
          <div key={uuid.v4()} className="logs-options--color-row">
            <div className="logs-options--color-column">
              <div className="logs-options--color-label">{lc.level}</div>
            </div>
            <div className="logs-options--color-column">
              <ColorDropdown
                selected={color}
                onChoose={onChangeSeverityLevel}
                stretchToFit={true}
                severityLevel={lc.level}
              />
            </div>
          </div>
        )
      })}
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
