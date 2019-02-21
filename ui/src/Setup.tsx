// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {connect} from 'react-redux'
import {InjectedRouter} from 'react-router'

// APIs
import {client} from 'src/utils/api'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

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

    const {allowed} = await client.setup.status()
    this.setState({
      loading: RemoteDataState.Done,
    })

    if (!allowed) {
      return
    }

    router.push('/onboarding/0')
  }

  public render() {
    const {loading} = this.state

    return (
      <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
        {this.props.children && React.cloneElement(this.props.children)}
      </SpinnerContainer>
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
