// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// constants
import {StepStatus, ConnectorState} from 'src/clockface/constants/wizard'

interface Props {
  currentStepIndex: number
  handleSetCurrentStep: (stepNumber: number) => void
  stepStatuses: StepStatus[]
  stepTitles: string[]
  stepTestIds: string[]
  stepSkippable: boolean[]
  hideFirstStep?: boolean
}

@ErrorHandling
class ProgressBar extends PureComponent<Props, null> {
  public render() {
    return <div className="wizard--progress-bar">{this.WizardProgress}</div>
  }

  private handleSetCurrentStep = (i: number) => () => {
    const {handleSetCurrentStep, currentStepIndex} = this.props

    const isAfterCurrentUnskippableStep =
      !this.isStepSkippable && i > currentStepIndex
    const isAfterNextUnskippableStep =
      this.nextNonSkippableStep !== -1 && i > this.nextNonSkippableStep

    const preventSkip =
      isAfterCurrentUnskippableStep || isAfterNextUnskippableStep

    if (preventSkip) {
      return
    }

    handleSetCurrentStep(i)
  }

  private get nextNonSkippableStep(): number {
    const {currentStepIndex, stepSkippable, stepStatuses} = this.props
    return _.findIndex(stepSkippable, (isSkippable, i) => {
      return (
        !isSkippable &&
        i > currentStepIndex &&
        stepStatuses[i] !== StepStatus.Complete
      )
    })
  }

  private get isStepSkippable(): boolean {
    const {stepSkippable, stepStatuses, currentStepIndex} = this.props

    return (
      stepSkippable[currentStepIndex] ||
      stepStatuses[currentStepIndex] === StepStatus.Complete
    )
  }

  private getStepClass(i: number): string {
    if (!this.isStepSkippable && i > this.props.currentStepIndex) {
      return 'wizard--progress-button unclickable'
    }
    return 'wizard--progress-button'
  }

  private get WizardProgress(): JSX.Element[] {
    const {
      hideFirstStep,
      stepStatuses,
      stepTitles,
      stepTestIds,
      currentStepIndex,
    } = this.props

    const lastIndex = stepStatuses.length - 1

    const progressBar: JSX.Element[] = stepStatuses.reduce(
      (acc, stepStatus, i) => {
        if (hideFirstStep && i === 0) {
          return [...acc]
        }

        let currentStep = ''

        // STEP STATUS
        if (i === currentStepIndex && stepStatus !== StepStatus.Error) {
          currentStep = 'current'
        }

        const stepEle = (
          <div
            key={`stepEle${i}`}
            className={this.getStepClass(i)}
            onClick={this.handleSetCurrentStep(i)}
          >
            <span
              className={`wizard--progress-icon ${currentStep || stepStatus}`}
            >
              <span className={`icon ${stepStatus}`} />
            </span>
            <div
              className={`wizard--progress-title ${currentStep || stepStatus}`}
              data-testid={stepTestIds[i]}
            >
              {stepTitles[i]}
            </div>
          </div>
        )

        if (i === lastIndex) {
          return [...acc, stepEle]
        }

        // PROGRESS BAR CONNECTOR
        let connectorStatus = ConnectorState.None

        if (i === currentStepIndex && stepStatus !== StepStatus.Error) {
          connectorStatus = ConnectorState.Some
        }
        if (i === lastIndex || stepStatus === StepStatus.Complete) {
          connectorStatus = ConnectorState.Full
        }
        const connectorEle = (
          <span
            key={i}
            className={`wizard--progress-connector wizard--progress-connector--${connectorStatus ||
              ConnectorState.None}`}
          />
        )
        return [...acc, stepEle, connectorEle]
      },
      []
    )
    return progressBar
  }
}

export default ProgressBar
