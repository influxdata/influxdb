// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {StepStatus} from 'src/clockface/constants/wizard'
import {Button, ComponentColor, ComponentSize} from 'src/clockface'

interface Props extends WithRouterProps {
  currentStepIndex: number
  handleSetCurrentStep: (stepNumber: number) => void
  handleSetStepStatus: (index: number, status: StepStatus) => void
  stepStatuses: StepStatus[]
  stepTitles: string[]
}

@ErrorHandling
class CompletionStep extends PureComponent<Props> {
  public render() {
    return (
      <div className="onboarding-step">
        <div className="splash-logo secondary" />
        <h3 className="wizard-step-title">Setup Complete! </h3>
        <p>"Start using the InfluxData platform in a few easy steps"</p>
        <p>This is Init Step </p>
        <div className="wizard-button-bar">
          <Button
            color={ComponentColor.Success}
            text="Go to status dashboard"
            size={ComponentSize.Large}
            onClick={this.handleComplete}
          />
        </div>
      </div>
    )
  }

  private handleComplete = () => {
    const {router} = this.props
    router.push(`/manage-sources`)
  }
}

export default withRouter(CompletionStep)
