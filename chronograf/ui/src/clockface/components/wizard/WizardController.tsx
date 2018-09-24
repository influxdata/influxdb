// Libraries
import React, {PureComponent, ReactElement} from 'react'

// Components
import WizardProgressBar from 'src/clockface/components/wizard/WizardProgressBar'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Step} from 'src/types/wizard'
import {WizardStepProps} from 'src/clockface/components/wizard/WizardStep'
import {StepStatus} from 'src/clockface/constants/wizard'
import {getDeep} from 'src/utils/wrappers'
import _ from 'lodash'

interface State {
  steps: Step[]
  currentStepIndex: number
}

interface Props {
  children: Array<ReactElement<WizardStepProps>>
  handleSkip?: () => void
  skipLinkText?: string
  jumpStep?: number
}

@ErrorHandling
class WizardController extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    skipLinkText: 'skip',
    jumpStep: -1,
  }

  public static getDerivedStateFromProps(props: Props, state: State) {
    let {currentStepIndex} = state
    const {children} = props
    const childSteps = React.Children.map(
      children,
      (child: ReactElement<WizardStepProps>, i) => {
        const isComplete = child.props.isComplete()
        let isErrored
        if (_.isBoolean(child.props.isErrored)) {
          isErrored = child.props.isErrored
        }
        if (_.isFunction(child.props.isErrored)) {
          isErrored = child.props.isErrored()
        }
        let stepStatus = StepStatus.Incomplete
        if (isComplete) {
          stepStatus = StepStatus.Complete
        }
        if (isErrored) {
          stepStatus = StepStatus.Error
        }

        if (currentStepIndex === -1 && !isComplete) {
          currentStepIndex = i
        }
        return {
          title: child.props.title,
          stepStatus,
        }
      }
    )

    if (currentStepIndex === -1) {
      currentStepIndex = childSteps.length - 1
    }
    return {steps: childSteps, currentStepIndex}
  }

  constructor(props: Props) {
    super(props)
    const {jumpStep} = this.props
    this.state = {
      steps: [],
      currentStepIndex: _.isNull(jumpStep) ? -1 : jumpStep,
    }
  }

  public render() {
    const {steps, currentStepIndex} = this.state
    const currentChild = this.CurrentChild
    return (
      <div className="wizard-controller">
        <div className="progress-header">
          <h3 className="wizard-step-title">{currentChild.props.title}</h3>
          <WizardProgressBar
            handleJump={this.jumpToStep}
            steps={steps}
            currentStepIndex={currentStepIndex}
          />
          {this.tipText}
        </div>
        {currentChild}
        {this.skipLink}
      </div>
    )
  }

  private incrementStep = () => {
    this.setState(prevState => {
      return {
        currentStepIndex: prevState.currentStepIndex + 1,
      }
    })
  }

  private decrementStep = () => {
    this.setState(prevState => {
      return {
        currentStepIndex: prevState.currentStepIndex - 1,
      }
    })
  }

  private jumpToStep = (jumpIndex: number) => () => {
    this.setState({
      currentStepIndex: jumpIndex,
    })
  }

  private get CurrentChild(): JSX.Element {
    const {children, handleSkip} = this.props
    const {currentStepIndex, steps} = this.state
    const lastStep = currentStepIndex === steps.length - 1

    const advance = lastStep ? handleSkip : this.incrementStep
    const retreat = currentStepIndex === 0 ? null : this.decrementStep

    let currentChild
    if (React.Children.count(children) === 1) {
      currentChild = children
    } else {
      currentChild = children[currentStepIndex]
    }
    return React.cloneElement<WizardStepProps>(currentChild, {
      increment: advance,
      decrement: retreat,
      lastStep,
    })
  }

  private get tipText() {
    const {currentStepIndex} = this.state
    const {children} = this.props

    let currentChild
    if (React.Children.count(children) === 1) {
      currentChild = children
    } else {
      currentChild = children[currentStepIndex]
    }

    const tipText = getDeep(currentChild, 'props.tipText', '')

    if (tipText) {
      return (
        <div className="wizard-tip-text">
          <p>{tipText}</p>
        </div>
      )
    }
    return null
  }

  private get skipLink() {
    const {handleSkip, skipLinkText} = this.props

    if (handleSkip) {
      return (
        <button
          className="btn btn-xs btn-primary btn-link wizard-skip-link"
          onClick={handleSkip}
        >
          {skipLinkText}
        </button>
      )
    }
    return null
  }
}

export default WizardController
