// Libraries
import React, {SFC} from 'react'

// Components
import {Button, ComponentColor, ComponentSize} from 'src/clockface'

interface Props {
  children: any
  currentStepIndex: number
  stepSkippable: boolean[]
  skipText?: string
  onSkip?: () => void
}

const WizardProgressHeader: SFC<Props> = (props: Props) => {
  const {children, skipText, onSkip, stepSkippable, currentStepIndex} = props
  return (
    <div className="wizard--progress-header">
      {children}
      {stepSkippable[currentStepIndex] && (
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
