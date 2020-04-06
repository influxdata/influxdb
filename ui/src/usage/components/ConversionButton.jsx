import React from 'react'
import {
  Button,
  ComponentColor,
  ComponentSize,
  FlexBox,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'

function ConversionButton() {
  const handlePayAsYouGo = () => (window.location.href = '/checkout')

  return (
    <FlexBox
      direction={FlexDirection.Row}
      justifyContent={JustifyContent.SpaceAround}
      margin={ComponentSize.Small}
    >
      <div>Want to remove these limits?</div>
      <Button
        color={ComponentColor.Primary}
        onClick={handlePayAsYouGo}
        text="Upgrade Now"
        size={ComponentSize.ExtraSmall}
      />
    </FlexBox>
  )
}

export default ConversionButton
