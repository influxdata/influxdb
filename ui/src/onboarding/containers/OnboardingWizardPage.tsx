// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import OnboardingWizard from 'src/onboarding/containers/OnboardingWizard'
import Notifications from 'src/shared/components/notifications/Notifications'

// Types
import {Notification, NotificationFunc, RemoteDataState} from 'src/types'
import {Links} from 'src/types/v2/links'

interface State {
  loading: RemoteDataState
  isSetupComplete: boolean
}

interface PassedProps {
  children: ReactElement<any>
  params: {
    stepID: string
  }
}

interface ConnectedStateProps {
  links: Links
}

interface ConnectedDispatchProps {
  notify: (message: Notification | NotificationFunc) => void
}

type Props = PassedProps &
  WithRouterProps &
  ConnectedStateProps &
  ConnectedDispatchProps

@ErrorHandling
export class OnboardingWizardPage extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
      isSetupComplete: false,
    }
  }

  public render() {
    const {params} = this.props

    return (
      <div className="chronograf-root">
        <Notifications inPresentationMode={true} />
        <OnboardingWizard
          onDecrementCurrentStepIndex={this.handleDecrementStepIndex}
          onIncrementCurrentStepIndex={this.handleIncrementStepIndex}
          onSetCurrentStepIndex={this.setStepIndex}
          onSetSubstepIndex={this.setSubstepIndex}
          currentStepIndex={+params.stepID}
          onCompleteSetup={this.handleCompleteSetup}
        />
      </div>
    )
  }

  public handleCompleteSetup = () => {
    this.setState({isSetupComplete: true})
  }

  private handleDecrementStepIndex = () => {
    const {
      params: {stepID},
    } = this.props

    this.setStepIndex(+stepID - 1)
  }

  private handleIncrementStepIndex = () => {
    const {
      params: {stepID},
    } = this.props

    this.setStepIndex(+stepID + 1)
  }

  private setStepIndex = (index: number) => {
    const {router} = this.props

    router.push(`/onboarding/${index}`)
  }

  private setSubstepIndex = (index: number, subStep: number | 'streaming') => {
    const {router} = this.props

    router.push(`/onboarding/${index}/${subStep}`)
  }
}

const mstp = ({links}) => ({links})

const mdtp = {
  notify: notifyAction,
}

export default connect<
  ConnectedStateProps,
  ConnectedDispatchProps,
  PassedProps
>(
  mstp,
  mdtp
)(withRouter<Props>(OnboardingWizardPage))
