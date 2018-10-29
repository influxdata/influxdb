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
}

@ErrorHandling
class ProgressBar extends PureComponent<Props, null> {
  public render() {
    return <div className="wizard--progress-bar">{this.WizardProgress}</div>
  }

  private handleSetCurrentStep = i => () => {
    const {handleSetCurrentStep} = this.props
    handleSetCurrentStep(i)
  }

  private get WizardProgress(): JSX.Element[] {
    const {stepStatuses, stepTitles, currentStepIndex} = this.props

    const lastIndex = stepStatuses.length - 1
    const lastEleIndex = stepStatuses.length - 2

    const progressBar: JSX.Element[] = stepStatuses.reduce(
      (acc, stepStatus, i) => {
        if (i === 0 || i === lastIndex) {
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
            className="wizard--progress-button"
            onClick={this.handleSetCurrentStep(i)}
          >
            <span
              className={`wizard--progress-icon ${currentStep || stepStatus}`}
            >
              <span className={`icon ${stepStatus}`} />
            </span>
            <div
              className={`wizard--progress-title ${currentStep || stepStatus}`}
            >
              {stepTitles[i]}
            </div>
          </div>
        )

        if (i === lastEleIndex) {
          return [...acc, stepEle]
        }

        // PROGRESS BAR CONNECTOR
        let connectorStatus = ConnectorState.None

        if (i === currentStepIndex && stepStatus !== StepStatus.Error) {
          connectorStatus = ConnectorState.Some
        }
        if (i === lastEleIndex || stepStatus === StepStatus.Complete) {
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
