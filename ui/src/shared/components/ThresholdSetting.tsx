// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {
  Input,
  SquareButton,
  IconFont,
  ComponentStatus,
  TextBlock,
  FlexBox,
  ComponentSize,
  FlexDirection,
  AlignItems,
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
    label = 'Value is >='
  }

  const isRemoveable =
    !isBaseThreshold && type !== COLOR_TYPE_MIN && type !== COLOR_TYPE_MAX

  const inputStatus = error ? ComponentStatus.Error : ComponentStatus.Default

  const dropdownStyle = isBaseThreshold
    ? {flex: '1 0 120px'}
    : {flex: '0 0 120px'}

  return (
    <>
      <FlexBox
        direction={FlexDirection.Row}
        alignItems={AlignItems.Center}
        margin={ComponentSize.Small}
        testID={id}
      >
        <TextBlock text={label} style={{flex: '0 0 90px'}} />
        {!isBaseThreshold && (
          <Input
            style={{flex: '1 0 0'}}
            testID={`threshold-${id}-input`}
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
          style={dropdownStyle}
        />
        {isRemoveable && (
          <SquareButton
            icon={IconFont.Remove}
            onClick={onRemove}
            style={{flex: '0 0 30px'}}
          />
        )}
      </FlexBox>
      {error && (
        <div
          className="threshold-setting--error"
          data-testid={`threshold-${id}-error`}
        >
          {error}
        </div>
      )}
    </>
  )
}

export default ThresholdSetting
