// Libraries
import React, {PureComponent, RefObject} from 'react'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ButtonType,
  ComponentStatus,
  Overlay,
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
    const {nextButtonText, nextButtonStatus, className} = this.props
    return (
      <Overlay.Footer className={className}>
        {this.backButton}
        <Button
          color={ComponentColor.Primary}
          text={nextButtonText}
          type={ButtonType.Submit}
          testID="next"
          ref={this.submitRef}
          status={nextButtonStatus}
          tabIndex={0}
        />
        {this.skipButton}
      </Overlay.Footer>
    )
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
