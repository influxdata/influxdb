// Libraries
import React, {FC} from 'react'

// Components
import {
  ComponentSpacerFlexChild,
  ComponentSize,
  ComponentStatus,
  AutoComplete,
  Input,
  InputType,
} from '@influxdata/clockface'
import {GreaterThreshold, LesserThreshold} from 'src/types'

// Types
interface Props {
  threshold: GreaterThreshold | LesserThreshold
  changeValue: (value: number) => void
}

const ThresholdValueStatement: FC<Props> = ({threshold, changeValue}) => {
  const onChangeValue = (e: React.ChangeEvent<HTMLInputElement>) => {
    changeValue(Number(e.target.value))
  }
  return (
    <ComponentSpacerFlexChild
      grow={1}
      shrink={0}
      testID="component-spacer--flex-child"
    >
      <Input
        autoFocus={false}
        onChange={onChangeValue}
        autocomplete={AutoComplete.Off}
        disabledTitleText="This input is disabled"
        name=""
        placeholder=""
        size={ComponentSize.Small}
        spellCheck={false}
        status={ComponentStatus.Default}
        testID="input-field"
        titleText=""
        type={InputType.Number}
        value={threshold.value}
      />
    </ComponentSpacerFlexChild>
  )
}

export default ThresholdValueStatement
