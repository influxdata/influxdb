// Libraries
import React, {FC} from 'react'

// Components
import {
  FlexBox,
  Panel,
  ComponentSize,
  ComponentStatus,
  AlignItems,
  PanelBody,
  TextBlock,
  SelectDropdown,
  DismissButton,
  ButtonType,
  ComponentColor,
  FlexDirection,
  JustifyContent,
  DropdownMenuTheme,
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
  const dropdownOptions = [
    'is above',
    'is below',
    'is inside range',
    'is outside range',
  ]

  const levelColor: string = LEVEL_COLORS[threshold.level]
  const selectedOption = OptionSelector(threshold)
  const onChangeThresholdType = (option: string) => {
    switch (option) {
      case 'is above':
        changeThresholdType('greater')
        break
      case 'is below':
        changeThresholdType('lesser')
        break
      case 'is inside range':
        changeThresholdType('range', true)
        break
      case 'is outside range':
        changeThresholdType('range', false)
        break
    }
  }
  return (
    <div
      style={{
        width: '475px',
      }}
    >
      <Panel
        backgroundColor="#292933"
        size={ComponentSize.Small}
        testID="panel"
      >
        <DismissButton
          active={false}
          color={ComponentColor.Warning}
          onClick={removeLevel}
          size={ComponentSize.ExtraSmall}
          status={ComponentStatus.Default}
          testID="dismiss-button"
          type={ButtonType.Button}
        />
        <PanelBody testID="panel--body">
          <FlexBox
            alignItems={AlignItems.Center}
            direction={FlexDirection.Column}
            justifyContent={JustifyContent.FlexStart}
            margin={ComponentSize.Small}
            stretchToFitHeight={false}
            stretchToFitWidth={false}
            testID="component-spacer"
          >
            <FlexBox
              alignItems={AlignItems.Center}
              direction={FlexDirection.Row}
              justifyContent={JustifyContent.FlexStart}
              margin={ComponentSize.Small}
              stretchToFitHeight={false}
              stretchToFitWidth
              testID="component-spacer"
            >
              <TextBlock
                monospace={false}
                size={ComponentSize.Small}
                testID="text-block"
                text="When value"
              />
              <FlexBox.FlexChild
                grow={2}
                shrink={0}
                testID="component-spacer--flex-child"
              >
                <SelectDropdown
                  buttonColor={ComponentColor.Default}
                  buttonSize={ComponentSize.Small}
                  buttonStatus={ComponentStatus.Default}
                  menuTheme={DropdownMenuTheme.Sapphire}
                  onSelect={onChangeThresholdType}
                  options={dropdownOptions}
                  selectedOption={selectedOption}
                  testID="select-dropdown"
                />
              </FlexBox.FlexChild>
            </FlexBox>
            <FlexBox
              alignItems={AlignItems.Center}
              direction={FlexDirection.Row}
              justifyContent={JustifyContent.FlexStart}
              margin={ComponentSize.Small}
              stretchToFitHeight={false}
              stretchToFitWidth
              testID="component-spacer"
            >
              {children}
              <TextBlock
                monospace={false}
                size={ComponentSize.Small}
                testID="text-block"
                text="set status to"
              />
              <TextBlock
                backgroundColor={levelColor}
                monospace={false}
                size={ComponentSize.Small}
                testID="text-block"
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
