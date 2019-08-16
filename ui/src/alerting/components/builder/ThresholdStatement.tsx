// Libraries
import React, {FC} from 'react'

// Components
import {
  FlexBox,
  Panel,
  ComponentSize,
  PanelBody,
  TextBlock,
  SelectDropdown,
  DismissButton,
  ButtonType,
  FlexDirection,
} from '@influxdata/clockface'

// Types
import {Threshold, ThresholdType} from 'src/types'
import {LEVEL_COLORS} from 'src/alerting/constants'

interface Props {
  threshold: Threshold
  removeLevel: () => void
  changeThresholdType: (toType: ThresholdType, within?: boolean) => void
}

const OptionSelector = (threshold: Threshold) => {
  if (threshold.type == 'greater') {
    return 'is above'
  }
  if (threshold.type == 'lesser') {
    return 'is below'
  }
  if (threshold.within) {
    return 'is inside range'
  }
  return 'is outside range'
}

const ThresholdStatement: FC<Props> = ({
  threshold,
  children,
  removeLevel,
  changeThresholdType,
}) => {
  const dropdownOptions = {
    ['is above']: 'greater',
    ['is below']: 'lesser',
    ['is inside range']: 'range',
    ['is outside range']: 'range',
  }

  const levelColor: string = LEVEL_COLORS[threshold.level]
  const selectedOption = OptionSelector(threshold)

  const onChangeThresholdType = (option: string) => {
    changeThresholdType(dropdownOptions[option], option === 'is inside range')
  }

  return (
    <div
      style={{
        width: '475px',
      }}
    >
      <Panel backgroundColor="#292933" testID="panel">
        <DismissButton
          onClick={removeLevel}
          testID="dismiss-button"
          type={ButtonType.Button}
        />
        <PanelBody testID="panel--body">
          <FlexBox
            direction={FlexDirection.Column}
            margin={ComponentSize.Small}
            testID="component-spacer"
          >
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
              stretchToFitWidth
              testID="component-spacer"
            >
              <TextBlock testID="when-value-text-block" text="When value" />
              <FlexBox.FlexChild grow={2} testID="component-spacer--flex-child">
                <SelectDropdown
                  options={Object.keys(dropdownOptions)}
                  selectedOption={selectedOption}
                  onSelect={onChangeThresholdType}
                  testID="select-option-dropdown"
                />
              </FlexBox.FlexChild>
            </FlexBox>
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
              stretchToFitWidth
              testID="component-spacer"
            >
              {children}
              <TextBlock
                testID="set-status-to-text-block"
                text="set status to"
              />
              <TextBlock
                backgroundColor={levelColor}
                testID="threshold-level-text-block"
                text={threshold.level}
              />
            </FlexBox>
          </FlexBox>
        </PanelBody>
      </Panel>
    </div>
  )
}

export default ThresholdStatement
