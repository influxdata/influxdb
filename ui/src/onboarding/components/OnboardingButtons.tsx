// Libraries
import React, {PureComponent, RefObject} from 'react'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ButtonType,
  ComponentStatus,
} from '@influxdata/clockface'

interface Props {
  onClickBack?: () => void
  skipButtonText?: string
  nextButtonText?: string
  backButtonText?: string
  onClickSkip?: () => void
  nextButtonStatus?: ComponentStatus
  showSkip?: boolean
  autoFocusNext?: boolean
  className?: string
}

class OnboardingButtons extends PureComponent<Props> {
  public static defaultProps: Props = {
    nextButtonStatus: ComponentStatus.Default,
    showSkip: false,
    autoFocusNext: true,
    skipButtonText: 'Skip',
    backButtonText: 'Previous',
    nextButtonText: 'Continue',
  }

  private submitRef: RefObject<HTMLButtonElement> = React.createRef()

  public componentDidMount() {
    if (this.props.autoFocusNext) {
      const buttonRef = this.submitRef.current
      if (buttonRef) {
        buttonRef.focus()
      }
    }
  }

  public componentDidUpdate() {
    if (this.props.autoFocusNext) {
      const buttonRef = this.submitRef.current
      if (buttonRef) {
        buttonRef.focus()
      }
    }
  }

  public render() {
    const {nextButtonText, nextButtonStatus} = this.props
    return (
      <div className={this.className}>
        <div className="wizard--button-bar">
          {this.backButton}
          <Button
            color={ComponentColor.Primary}
            text={nextButtonText}
            size={ComponentSize.Medium}
            type={ButtonType.Submit}
            testID="next"
            refObject={this.submitRef}
            status={nextButtonStatus}
            tabIndex={0}
          />
        </div>
        {this.skipButton}
      </div>
    )
  }

  private get className(): string {
    return this.props.className || ''
  }

  private get backButton(): JSX.Element {
    const {backButtonText, onClickBack} = this.props

    if (!onClickBack || !backButtonText) {
      return
    }

    return (
      <Button
        color={ComponentColor.Default}
        text={backButtonText}
        size={ComponentSize.Medium}
        onClick={onClickBack}
        testID="back"
        tabIndex={1}
      />
    )
  }

  private get skipButton(): JSX.Element {
    const {skipButtonText, onClickSkip, showSkip} = this.props
    if (!showSkip) {
      return
    }

    return (
      <div className="wizard--skip-container">
        <Button
          className="wizard--skip-button"
          size={ComponentSize.Medium}
          color={ComponentColor.Default}
          text={skipButtonText}
          onClick={onClickSkip}
          testID="skip"
        />
      </div>
    )
  }
}

export default OnboardingButtons
