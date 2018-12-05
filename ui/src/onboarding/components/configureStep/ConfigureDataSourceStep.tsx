// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from 'src/clockface'
import ConfigureDataSourceSwitcher from 'src/onboarding/components/configureStep/ConfigureDataSourceSwitcher'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {TelegrafPlugin, DataLoaderType} from 'src/types/v2/dataLoaders'

export interface Props extends OnboardingStepProps {
  telegrafPlugins: TelegrafPlugin[]
  type: DataLoaderType
}

interface State {
  currentDataSourceIndex: number
}

@ErrorHandling
class ConfigureDataSourceStep extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      currentDataSourceIndex: 0,
    }
  }

  public render() {
    const {telegrafPlugins, type} = this.props

    return (
      <div className="onboarding-step">
        <ConfigureDataSourceSwitcher
          telegrafPlugins={telegrafPlugins}
          currentIndex={this.state.currentDataSourceIndex}
          dataLoaderType={type}
        />
        <div className="wizard-button-container">
          <div className="wizard-button-bar">
            <Button
              color={ComponentColor.Default}
              text="Back"
              size={ComponentSize.Medium}
              onClick={this.handlePrevious}
            />
            <Button
              color={ComponentColor.Primary}
              text="Next"
              size={ComponentSize.Medium}
              onClick={this.handleNext}
              status={ComponentStatus.Default}
              titleText={'Next'}
            />
          </div>
          {this.skipLink}
        </div>
      </div>
    )
  }

  private get skipLink() {
    return (
      <Button
        color={ComponentColor.Default}
        text="Skip"
        size={ComponentSize.Small}
        onClick={this.jumpToCompletionStep}
      >
        skip
      </Button>
    )
  }

  private jumpToCompletionStep = () => {
    const {onSetCurrentStepIndex, stepStatuses} = this.props

    onSetCurrentStepIndex(stepStatuses.length - 1)
  }

  private handleNext = () => {
    const {onIncrementCurrentStepIndex, telegrafPlugins} = this.props
    const {currentDataSourceIndex} = this.state

    if (currentDataSourceIndex >= telegrafPlugins.length - 1) {
      onIncrementCurrentStepIndex()
    } else {
      this.setState({currentDataSourceIndex: currentDataSourceIndex + 1})
    }
  }

  private handlePrevious = () => {
    const {onDecrementCurrentStepIndex} = this.props
    const {currentDataSourceIndex} = this.state

    if (currentDataSourceIndex === 0) {
      onDecrementCurrentStepIndex()
    } else {
      this.setState({currentDataSourceIndex: currentDataSourceIndex - 1})
    }
  }
}

export default ConfigureDataSourceStep
