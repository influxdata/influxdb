// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {connect} from 'react-redux'

// APIs
import {getSetupStatus} from 'src/onboarding/apis'

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

interface Props {
  links: Links
  children: ReactElement<any>
  notify: (message: Notification | NotificationFunc) => void
}

@ErrorHandling
export class Setup extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
      isSetupComplete: false,
    }
  }

  public async componentDidMount() {
    const {links} = this.props
    const isSetupAllowed = await getSetupStatus(links.setup)
    this.setState({
      loading: RemoteDataState.Done,
      isSetupComplete: !isSetupAllowed,
    })
  }

  public render() {
    const {isSetupComplete} = this.state
    if (this.isLoading) {
      return <div className="page-spinner" />
    }
    if (!isSetupComplete) {
      return (
        <div className="chronograf-root">
          <Notifications inPresentationMode={true} />
          <OnboardingWizard onCompleteSetup={this.handleCompleteSetup} />
        </div>
      )
    } else {
      return this.props.children && React.cloneElement(this.props.children)
    }
  }

  public handleCompleteSetup = () => {
    this.setState({isSetupComplete: true})
  }

  private get isLoading(): boolean {
    const {loading} = this.state
    return (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.NotStarted
    )
  }
}

const mstp = ({links}) => ({links})

const mdtp = {
  notify: notifyAction,
}

export default connect(mstp, mdtp)(Setup)
