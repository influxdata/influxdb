// Libraries
import React, {FC} from 'react'

// Components
import {FlexBox, Input, InputType} from '@influxdata/clockface'
import {GreaterThreshold, LesserThreshold} from 'src/types'

// Utils
import {convertUserInputToNumOrNaN} from 'src/shared/utils/convertUserInput'

// Types
interface Props {
  threshold: GreaterThreshold | LesserThreshold
  changeValue: (value: number) => void
}

const ThresholdValueStatement: FC<Props> = ({threshold, changeValue}) => {
  const onChangeValue = (e: React.ChangeEvent<HTMLInputElement>) => {
    changeValue(convertUserInputToNumOrNaN(e))
  }
  return (
    <FlexBox.Child testID="component-spacer--flex-child">
      <Input
        onChange={onChangeValue}
        name=""
        testID="input-field"
        type={InputType.Number}
        value={threshold.value}
      />
    </FlexBox.Child>
  )
}

export default ThresholdValueStatement
