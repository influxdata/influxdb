// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {
  FlexBox,
  FlexDirection,
  AlignItems,
  ComponentColor,
  ComponentSize,
} from '@influxdata/clockface'
import DashedButton from 'src/shared/components/dashed_button/DashedButton'

const CheckThresholdsCard: FunctionComponent = () => {
  return (
    <FlexBox
      direction={FlexDirection.Column}
      alignItems={AlignItems.Stretch}
      margin={ComponentSize.Medium}
    >
      <DashedButton
        text="+ INFO"
        color={ComponentColor.Success}
        size={ComponentSize.Large}
        onClick={() => {}}
      />
      <DashedButton
        text="+ WARN"
        color={ComponentColor.Warning}
        size={ComponentSize.Large}
        onClick={() => {}}
      />
      <DashedButton
        text="+ CRIT"
        color={ComponentColor.Danger}
        size={ComponentSize.Large}
        onClick={() => {}}
      />
    </FlexBox>
  )
}

export default CheckThresholdsCard
