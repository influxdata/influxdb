// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {
  Input,
  Button,
  ButtonShape,
  IconFont,
  ComponentStatus,
} from '@influxdata/clockface'
import ColorDropdown from 'src/shared/components/ColorDropdown'

// Constants
import {
  THRESHOLD_COLORS,
  BASE_THRESHOLD_ID,
  COLOR_TYPE_MIN,
  COLOR_TYPE_MAX,
} from 'src/shared/constants/thresholds'

interface Props {
  id: string
  type: string
  name: string
  value: string
  error?: string
  onChangeValue: (value: string) => void
  onChangeColor: (name: string, hex: string) => void
  onRemove: () => void
  onBlur: () => void
}

const ThresholdSetting: FunctionComponent<Props> = ({
  id,
  type,
  name,
  value,
  error,
  onChangeValue,
  onChangeColor,
  onRemove,
  onBlur,
}) => {
  const isBaseThreshold = id === BASE_THRESHOLD_ID

  let label: string = ''

  if (isBaseThreshold) {
    label = 'Base'
  } else if (type === COLOR_TYPE_MIN) {
    label = 'Minimum'
  } else if (type === COLOR_TYPE_MAX) {
    label = 'Maximum'
  } else {
    label = 'Value is <='
  }

  const isRemoveable =
    !isBaseThreshold && type !== COLOR_TYPE_MIN && type !== COLOR_TYPE_MAX

  const inputStatus = error ? ComponentStatus.Error : ComponentStatus.Default

  return (
    <div className="threshold-setting" data-test-id={id}>
      <div className="threshold-setting--controls">
        <div className="threshold-setting--label">{label}</div>
        {!isBaseThreshold && (
          <Input
            className="threshold-setting--value"
            value={value}
            status={inputStatus}
            onChange={e => onChangeValue(e.target.value)}
            onBlur={onBlur}
            onKeyDown={e => {
              if (e.key === 'Enter') {
                onBlur()
              }
            }}
          />
        )}
        <ColorDropdown
          colors={THRESHOLD_COLORS}
          selected={THRESHOLD_COLORS.find(d => d.name === name)}
          onChoose={({name, hex}) => onChangeColor(name, hex)}
          stretchToFit={true}
        />
        {isRemoveable && (
          <Button
            className="threshold-setting--remove"
            icon={IconFont.Remove}
            shape={ButtonShape.Square}
            onClick={onRemove}
          />
        )}
      </div>
      {error && <div className="threshold-setting--error">{error}</div>}
    </div>
  )
}

export default ThresholdSetting
