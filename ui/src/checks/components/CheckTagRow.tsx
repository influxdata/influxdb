// Libraries
import React, {FC} from 'react'

// Components
import {
  Input,
  Panel,
  DismissButton,
  TextBlock,
  FlexBox,
  ComponentSize,
  FlexDirection,
  ComponentColor,
} from '@influxdata/clockface'

// Types
import {CheckTagSet} from 'src/types'

interface Props {
  index: number
  tagSet: CheckTagSet
  handleChangeTagRow: (i: number, tags: CheckTagSet) => void
  handleRemoveTagRow: (i: number) => void
}

const CheckTagRow: FC<Props> = ({
  tagSet,
  index,
  handleChangeTagRow,
  handleRemoveTagRow,
}) => {
  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    handleChangeTagRow(index, {...tagSet, [e.target.name]: e.target.value})
  }

  return (
    <Panel testID="tag-rule" className="alert-builder--tag-row">
      <DismissButton
        onClick={() => {
          handleRemoveTagRow(index)
        }}
        color={ComponentColor.Default}
      />
      <Panel.Body size={ComponentSize.ExtraSmall}>
        <FlexBox direction={FlexDirection.Row} margin={ComponentSize.Small}>
          <FlexBox.Child grow={1}>
            <Input
              testID="tag-rule-key--input"
              placeholder="Tag"
              value={tagSet.key}
              name="key"
              onChange={handleChange}
            />
          </FlexBox.Child>
          <FlexBox.Child grow={0} basis={20}>
            <TextBlock text="=" />
          </FlexBox.Child>
          <FlexBox.Child grow={1}>
            <Input
              testID="tag-rule-key--input"
              placeholder="Value"
              value={tagSet.value}
              name="value"
              onChange={handleChange}
            />
          </FlexBox.Child>
        </FlexBox>
      </Panel.Body>
    </Panel>
  )
}

export default CheckTagRow
