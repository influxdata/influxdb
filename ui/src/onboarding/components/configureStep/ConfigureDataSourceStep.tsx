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
import {DataSource, DataLoaderType} from 'src/types/v2/dataLoaders'

export interface Props extends OnboardingStepProps {
  dataSources: DataSource[]
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
    const {setupParams, dataSources} = this.props

    return (
      <div className="onboarding-step">
        <ConfigureDataSourceSwitcher
          dataSources={dataSources}
          currentIndex={this.state.currentDataSourceIndex}
          org={_.get(setupParams, 'org', '')}
          username={_.get(setupParams, 'username', '')}
          bucket={_.get(setupParams, 'bucket', '')}
        />
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

    if (currentDataSourceIndex >= dataSources.length) {
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
