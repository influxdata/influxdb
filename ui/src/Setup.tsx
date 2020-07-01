// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {Switch, Route, RouteComponentProps} from 'react-router-dom'

// APIs
import {client} from 'src/utils/api'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import OnboardingWizardPage from 'src/onboarding/containers/OnboardingWizardPage'
import UnauthenticatedApp from 'src/shared/containers/UnauthenticatedApp'
import Signin from 'src/Signin'
import GetMe from 'src/shared/containers/GetMe'
import GetFlags from 'src/shared/containers/GetFlags'
import GetOrganizations from 'src/shared/containers/GetOrganizations'

// Utils
import {isOnboardingURL} from 'src/onboarding/utils'

// Types
import {RemoteDataState} from 'src/types'

interface State {
  loading: RemoteDataState
  isSetupComplete: boolean
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
      isSetupComplete: false,
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
    })

    if (!allowed) {
      return
    }

    history.push('/onboarding/0')
  }

  public render() {
    const {loading} = this.state

    return (
      <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
        <Switch>
          <Route path="/onboarding/:stepID" component={OnboardingWizardPage} />
          <Route
            path="/onboarding/:stepID/:substepID"
            component={OnboardingWizardPage}
          />
          <Route component={UnauthenticatedApp} />
        </Switch>
        <Signin>
          <GetMe>
            <GetFlags>
              <GetOrganizations />
            </GetFlags>
          </GetMe>
        </Signin>
      </SpinnerContainer>
    )
  }
}

export default Setup
