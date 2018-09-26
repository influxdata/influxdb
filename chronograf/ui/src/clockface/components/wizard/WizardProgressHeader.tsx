// Libraries
import React, {SFC} from 'react'

// Components
import {Button, ComponentColor, ComponentSize} from 'src/clockface'

interface Props {
  children: any
  skipText?: string
  onSkip?: () => void
}

const WizardProgressHeader: SFC<Props> = (props: Props) => {
  const {children, skipText, onSkip} = props

  return (
    <div className="wizard--progress-header">
      {children}
      {!!onSkip && (
        <span className="wizard--progress-skip">
          <Button
            color={ComponentColor.Default}
            text={skipText}
            size={ComponentSize.Small}
            onClick={onSkip}
          />
        </span>
      )}
    </div>
  )
}

WizardProgressHeader.defaultProps = {
  skipText: 'Skip',
  onSkip: () => {},
}

export default WizardProgressHeader
