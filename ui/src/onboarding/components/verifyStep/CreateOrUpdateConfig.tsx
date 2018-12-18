// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Spinner} from 'src/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {createOrUpdateTelegrafConfigAsync} from 'src/onboarding/actions/dataLoaders'

// Constants
import {
  TelegrafConfigCreationSuccess,
  TelegrafConfigCreationError,
} from 'src/shared/copy/notifications'

// Types
import {RemoteDataState} from 'src/types'

export interface Props {
  org: string
  authToken: string
  children: () => JSX.Element
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
}

interface State {
  loading: RemoteDataState
}

@ErrorHandling
class CreateOrUpdateConfig extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {loading: RemoteDataState.NotStarted}
  }

  public async componentDidMount() {
    const {onSaveTelegrafConfig, authToken} = this.props

    this.setState({loading: RemoteDataState.Loading})

    try {
      await onSaveTelegrafConfig(authToken)
      notify(TelegrafConfigCreationSuccess)

      this.setState({loading: RemoteDataState.Done})
    } catch (error) {
      notify(TelegrafConfigCreationError)
    }
  }

  public render() {
    return (
      <Spinner loading={this.state.loading}>{this.props.children()}</Spinner>
    )
  }
}

export default CreateOrUpdateConfig
