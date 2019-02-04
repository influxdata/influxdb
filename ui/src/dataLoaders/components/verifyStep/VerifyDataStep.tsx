// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import VerifyDataSwitcher from 'src/dataLoaders/components/verifyStep/VerifyDataSwitcher'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {Form} from 'src/clockface'
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'

export interface OwnProps extends DataLoaderStepProps {
  type: DataLoaderType
  stepIndex: number
}

interface StateProps {
  lpStatus: RemoteDataState
}

export type Props = OwnProps & StateProps

@ErrorHandling
export class VerifyDataStep extends PureComponent<Props> {
  public render() {
    const {type, onDecrementCurrentStepIndex, lpStatus} = this.props

    return (
      <div className="onboarding-step">
        <Form onSubmit={this.handleIncrementStep}>
          <div className="wizard-step--scroll-area">
            <FancyScrollbar autoHide={false}>
              <div className="wizard-step--scroll-content">
                <VerifyDataSwitcher
                  type={type}
                  onDecrementCurrentStep={onDecrementCurrentStepIndex}
                  lpStatus={lpStatus}
                />
              </div>
            </FancyScrollbar>
          </div>
          <OnboardingButtons
            onClickBack={this.handleDecrementStep}
            nextButtonText={'Finish'}
          />
        </Form>
      </div>
    )
  }

  private handleIncrementStep = () => {
    const {onExit} = this.props
    onExit()
  }

  private handleDecrementStep = () => {
    const {onDecrementCurrentStepIndex} = this.props

    onDecrementCurrentStepIndex()
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {lpStatus},
  },
}: AppState): StateProps => ({
  lpStatus,
})

export default connect<StateProps, {}, OwnProps>(mstp)(VerifyDataStep)
