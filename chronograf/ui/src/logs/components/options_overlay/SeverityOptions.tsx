import React, {SFC} from 'react'
import uuid from 'uuid'

import {Button, IconFont, ButtonShape} from 'src/clockface'
import ColorDropdown from 'src/shared/components/color_dropdown/ColorDropdown'
import SeverityColumnFormat from 'src/logs/components/options_overlay/SeverityColumnFormat'

import {
  SeverityLevelColor,
  SeverityColor,
  SeverityFormat,
  SeverityColorValues,
} from 'src/types/logs'

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
    <Button
      shape={ButtonShape.Default}
      icon={IconFont.Refresh}
      onClick={onReset}
      text="Reset to Defaults"
    />
    <SeverityColumnFormat
      format={severityFormat}
      onChangeFormat={onChangeSeverityFormat}
    />
  </>
)

export default SeverityConfig
