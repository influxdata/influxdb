// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from 'src/clockface'
import ConfigureDataSourceSwitcher from 'src/onboarding/components/ConfigureDataSourceSwitcher'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

export interface Props extends OnboardingStepProps {
  dataSources: string[]
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
    const {dataSources} = this.props
    const {currentDataSourceIndex} = this.state
    const dataSource = dataSources[currentDataSourceIndex] || 'yo'

    return (
      <div className="onboarding-step">
        <ConfigureDataSourceSwitcher dataSource={dataSource} />

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
      </div>
    )
  }

  private handleNext = () => {
    const {onIncrementCurrentStepIndex, dataSources} = this.props
    const {currentDataSourceIndex} = this.state

    if (currentDataSourceIndex >= dataSources.length - 1) {
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
