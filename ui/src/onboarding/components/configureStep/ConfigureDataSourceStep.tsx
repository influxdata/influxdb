// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {withRouter, WithRouterProps} from 'react-router'

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

export interface OwnProps extends OnboardingStepProps {
  telegrafPlugins: TelegrafPlugin[]
  type: DataLoaderType
}

interface RouterProps {
  params: {
    stepID: string
    substepID: string
  }
}

type Props = OwnProps & WithRouterProps & RouterProps

@ErrorHandling
class ConfigureDataSourceStep extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public componentDidMount() {
    const {
      router,
      params: {stepID, substepID},
    } = this.props

    if (substepID === undefined) {
      router.replace(`/onboarding/${stepID}/0`)
    }
  }

  public render() {
    const {
      telegrafPlugins,
      type,
      params: {substepID},
      setupParams,
    } = this.props

    return (
      <div className="onboarding-step">
        <ConfigureDataSourceSwitcher
          bucket={_.get(setupParams, 'bucket', '')}
          org={_.get(setupParams, 'org', '')}
          telegrafPlugins={telegrafPlugins}
          dataLoaderType={type}
          currentIndex={+substepID}
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
    const {
      onIncrementCurrentStepIndex,
      telegrafPlugins,
      params: {substepID, stepID},
      router,
    } = this.props

    const index = +substepID

    if (index >= telegrafPlugins.length - 1) {
      onIncrementCurrentStepIndex()
    } else {
      router.push(`/onboarding/${stepID}/${index + 1}`)
    }
  }

  private handlePrevious = () => {
    const {router} = this.props

    router.goBack()
  }
}

export default withRouter<OwnProps>(ConfigureDataSourceStep)
