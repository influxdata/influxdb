// Libraries
import React, {PureComponent} from 'react'
import {Switch, Route} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import GetFlags from 'src/shared/containers/GetFlags'

// Types
import {RemoteDataState} from 'src/types'

// Actions
import {getMe} from 'src/shared/actions/me'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface ConnectDispatchProps {
  getMe: typeof getMe
}

interface State {
  loading: RemoteDataState
}

type Props = ConnectDispatchProps

@ErrorHandling
class GetMe extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
    }
  }

  public render() {
    const {loading} = this.state

    return (
      <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
        <Switch>
          <Route render={props => <GetFlags {...props} />} />
        </Switch>
      </SpinnerContainer>
    )
  }

  public componentDidMount() {
    this.props.getMe()
    this.setState({loading: RemoteDataState.Done})
  }
}

const mdtp = {
  getMe,
}

export default connect<{}, ConnectDispatchProps>(null, mdtp)(GetMe)
