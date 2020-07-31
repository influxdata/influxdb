// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {Switch, Route, RouteComponentProps} from 'react-router-dom'

// APIs
import {client} from 'src/utils/api'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import GetAppState from 'src/shared/containers/GetAppState'
import OnboardingWizardPage from 'src/onboarding/containers/OnboardingWizardPage'
import SigninPage from 'src/onboarding/containers/SigninPage'
import {LoginPage} from 'src/onboarding/containers/LoginPage'
import Logout from 'src/Logout'

// Constants
import {LOGIN, SIGNIN, LOGOUT} from 'src/shared/constants/routes'

// Utils
import {isOnboardingURL} from 'src/onboarding/utils'

// Types
import {RemoteDataState} from 'src/types'

interface State {
  loading: RemoteDataState
  allowed: boolean
}

interface OwnProps {
  children: ReactElement<any>
}

type Props = RouteComponentProps & OwnProps

@ErrorHandling
export class Setup extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
      allowed: false,
    }
  }

  public async componentDidMount() {
    const {history} = this.props

    if (isOnboardingURL()) {
      this.setState({
        loading: RemoteDataState.Done,
      })
      return
    }

    const {allowed} = await client.setup.status()
    this.setState({
      loading: RemoteDataState.Done,
      allowed,
    })

    if (!allowed) {
      return
    }

    history.push('/onboarding/0')
  }

  async componentDidUpdate(prevProps: Props, prevState: State) {
    if (!prevState.allowed) {
      return
    }

    if (prevProps.location.pathname.includes('/onboarding/2')) {
      this.setState({loading: RemoteDataState.Loading})
      const {allowed} = await client.setup.status()
      this.setState({allowed, loading: RemoteDataState.Done})
    }
  }

  public render() {
    const {loading, allowed} = this.state

    return (
      <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
        {allowed && (
          <Route path="/onboarding/:stepID" component={OnboardingWizardPage} />
        )}
        {!allowed && (
          <Switch>
            <Route
              path="/onboarding/:stepID"
              component={OnboardingWizardPage}
            />
            <Route path={LOGIN} component={LoginPage} />
            <Route path={SIGNIN} component={SigninPage} />
            <Route path={LOGOUT} component={Logout} />
            <Route component={GetAppState} />
          </Switch>
        )}
      </SpinnerContainer>
    )
  }
}

export default Setup
