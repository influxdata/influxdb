// Libraries
import React, {PureComponent, RefObject} from 'react'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ButtonType,
  ComponentStatus,
} from 'src/clockface'

interface Props extends DefaultProps {
  onClickBack: () => void
  nextButtonText: string
  backButtonText: string
  onClickSkip?: () => void
}

interface DefaultProps {
  nextButtonStatus?: ComponentStatus
  showSkip?: boolean
  autoFocusNext?: boolean
  skipButtonText?: string
}

class OnboardingButtons extends PureComponent<Props> {
  public static defaultProps: DefaultProps = {
    nextButtonStatus: ComponentStatus.Default,
    showSkip: false,
    autoFocusNext: true,
    skipButtonText: 'Skip',
  }

  private submitRef: RefObject<Button> = React.createRef()

  public componentDidMount() {
    if (this.props.autoFocusNext) {
      const buttonRef = this.submitRef.current.ref
      if (buttonRef) {
        buttonRef.current.focus()
      }
    }
  }

  public componentDidUpdate() {
    if (this.props.autoFocusNext) {
      const buttonRef = this.submitRef.current.ref
      if (buttonRef) {
        buttonRef.current.focus()
      }
    }
  }

  public render() {
    const {
      backButtonText,
      nextButtonText,
      onClickBack,
      nextButtonStatus,
    } = this.props
    return (
      <div className="wizard--button-container">
        <div className="wizard--button-bar">
          <Button
            color={ComponentColor.Default}
            text={backButtonText}
            size={ComponentSize.Medium}
            onClick={onClickBack}
            data-test="back"
            tabIndex={1}
          />
          <Button
            color={ComponentColor.Primary}
            text={nextButtonText}
            size={ComponentSize.Medium}
            type={ButtonType.Submit}
            data-test="next"
            ref={this.submitRef}
            status={nextButtonStatus}
            tabIndex={0}
          />
        </div>
        {this.skipButton}
      </div>
    )
  }

  private get skipButton(): JSX.Element {
    const {skipButtonText, onClickSkip, showSkip} = this.props
    if (!showSkip) {
      return
    }

    return (
      <Button
        customClass="wizard--skip-button"
        size={ComponentSize.Medium}
        color={ComponentColor.Default}
        text={skipButtonText}
        onClick={onClickSkip}
        data-test="skip"
      />
    )
  }
}

export default OnboardingButtons
