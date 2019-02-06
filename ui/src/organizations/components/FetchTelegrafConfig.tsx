// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {SpinnerContainer, TechnoSpinner} from 'src/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Apis
import {client} from 'src/utils/api'

// Constants
import {getTelegrafConfigFailed} from 'src/shared/copy/v2/notifications'

// types
import {RemoteDataState} from 'src/types'
import {notify as notifyAction} from 'src/shared/actions/notifications'

export interface Props {
  telegrafConfigID: string
  notify: typeof notifyAction
  children: (telegrafConfig: string) => JSX.Element
}

interface State {
  loading: RemoteDataState
  telegrafConfig?: string
}

@ErrorHandling
class FetchTelegrafConfig extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {loading: RemoteDataState.NotStarted}
  }

  public async componentDidMount() {
    const {telegrafConfigID, notify} = this.props

    this.setState({loading: RemoteDataState.Loading})
    try {
      const telegrafConfig = await client.telegrafConfigs.getTOML(
        telegrafConfigID
      )
      this.setState({telegrafConfig, loading: RemoteDataState.Done})
    } catch (err) {
      this.setState({loading: RemoteDataState.Error})
      notify(getTelegrafConfigFailed())
    }
  }

  public render() {
    return (
      <SpinnerContainer
        loading={this.state.loading}
        spinnerComponent={<TechnoSpinner />}
      >
        {this.props.children(this.state.telegrafConfig)}
      </SpinnerContainer>
    )
  }
}

export default FetchTelegrafConfig
