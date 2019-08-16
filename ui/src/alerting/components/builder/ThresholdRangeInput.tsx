// Libraries
import React, {FC} from 'react'

// Components
import {
  ComponentSpacerFlexChild,
  ComponentSize,
  ComponentStatus,
  AutoComplete,
  TextBlock,
  Input,
  InputType,
} from '@influxdata/clockface'
import {RangeThreshold} from 'src/types'

// Types
interface Props {
  threshold: RangeThreshold
  changeRange: (min: number, max: number) => void
}

const ThresholdRangeStatement: FC<Props> = ({threshold, changeRange}) => {
  const onChangeMin = (e: React.ChangeEvent<HTMLInputElement>) => {
    const min = Number(e.target.value)
    changeRange(min, threshold.max)
  }

  const onChangeMax = (e: React.ChangeEvent<HTMLInputElement>) => {
    const max = Number(e.target.value)
    changeRange(threshold.min, max)
  }

  return (
    <>
      <ComponentSpacerFlexChild
        grow={1}
        shrink={0}
        testID="component-spacer--flex-child"
      >
        <Input
          onChange={onChangeMin}
          autoFocus={false}
          autocomplete={AutoComplete.Off}
          disabledTitleText="This input is disabled"
          name="min"
          placeholder=""
          size={ComponentSize.Small}
          spellCheck={false}
          status={ComponentStatus.Default}
          testID="input-field"
          titleText=""
          type={InputType.Number}
          value={threshold.min}
        />
      </ComponentSpacerFlexChild>
      <TextBlock
        monospace={false}
        size={ComponentSize.Small}
        testID="text-block"
        text="to"
      />
      <ComponentSpacerFlexChild
        grow={1}
        shrink={0}
        testID="component-spacer--flex-child"
      >
        <Input
          onChange={onChangeMax}
          autoFocus={false}
          autocomplete={AutoComplete.Off}
          disabledTitleText="This input is disabled"
          name="max"
          placeholder=""
          size={ComponentSize.Small}
          spellCheck={false}
          status={ComponentStatus.Default}
          testID="input-field"
          titleText=""
          type={InputType.Number}
          value={threshold.max}
        />
      </ComponentSpacerFlexChild>
    </>
  )
}

export default ThresholdRangeStatement
