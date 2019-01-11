// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {connect} from 'react-redux'
import {InjectedRouter} from 'react-router'

// APIs
import {getSetupStatus} from 'src/onboarding/apis'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Utils
import {isOnboardingURL} from 'src/onboarding/utils'

// Types
import {Notification, NotificationFunc, RemoteDataState} from 'src/types'

interface State {
  loading: RemoteDataState
  isSetupComplete: boolean
}

interface Props {
  router: InjectedRouter
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
    const {router} = this.props

    if (isOnboardingURL()) {
      this.setState({
        loading: RemoteDataState.Done,
      })
      return
    }

    const isSetupAllowed = await getSetupStatus()
    this.setState({
      loading: RemoteDataState.Done,
    })

    if (!isSetupAllowed) {
      return
    }

    router.push('/onboarding/0')
  }

  public render() {
    if (this.isLoading) {
      return <div className="page-spinner" />
    } else {
      return this.props.children && React.cloneElement(this.props.children)
    }
  }

  private get isLoading(): boolean {
    const {loading} = this.state
    return (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.NotStarted
    )
  }
}

const mdtp = {
  notify: notifyAction,
}

export default connect(
  null,
  mdtp
)(Setup)
