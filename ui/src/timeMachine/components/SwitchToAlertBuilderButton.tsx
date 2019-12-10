// Libraries
import React, {FC} from 'react'

// Components
import {
  ComponentColor,
  Appearance,
  ConfirmationButton,
} from '@influxdata/clockface'

const SwitchToAlertBuilderButton: FC = () => {
  const switchToBuilder = () => {
    console.log('weee')
  }
  return (
    <ConfirmationButton
      popoverColor={ComponentColor.Danger}
      popoverAppearance={Appearance.Outline}
      popoverStyle={{width: '400px'}}
      confirmationLabel="Returning to Alert Builder mode will discard any changes you
          have made using Flux. This cannot be recovered."
      confirmationButtonText="Return to Alert Builder"
      text="Return to Alert Builder"
      onConfirm={switchToBuilder}
      testID="switch-alert-builder-confirm"
    />
  )
}

export default SwitchToAlertBuilderButton
