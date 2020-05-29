// Libraries
import React, {FC} from 'react'

// Components
import {FlexBox, TextBlock, Input, InputType} from '@influxdata/clockface'
import {RangeThreshold} from 'src/types'

// Utils
import {convertUserInputToNumOrNaN} from 'src/shared/utils/convertUserInput'

// Types
interface Props {
  threshold: RangeThreshold
  changeRange: (min: number, max: number) => void
}

const ThresholdRangeStatement: FC<Props> = ({threshold, changeRange}) => {
  const onChangeMin = (e: React.ChangeEvent<HTMLInputElement>) => {
    const min = convertUserInputToNumOrNaN(e)

    changeRange(min, threshold.max)
  }

  const onChangeMax = (e: React.ChangeEvent<HTMLInputElement>) => {
    const max = convertUserInputToNumOrNaN(e)
    changeRange(threshold.min, max)
  }

  return (
    <>
      <FlexBox.Child testID="component-spacer--flex-child">
        <Input
          onChange={onChangeMin}
          name="min"
          testID="input-field"
          type={InputType.Number}
          value={threshold.min}
        />
      </FlexBox.Child>
      <TextBlock testID="text-block" text="to" />
      <FlexBox.Child testID="component-spacer--flex-child">
        <Input
          onChange={onChangeMax}
          disabledTitleText="This input is disabled"
          name="max"
          testID="input-field"
          type={InputType.Number}
          value={threshold.max}
        />
      </FlexBox.Child>
    </>
  )
}

export default ThresholdRangeStatement
