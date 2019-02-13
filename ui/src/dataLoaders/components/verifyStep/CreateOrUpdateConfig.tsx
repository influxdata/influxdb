// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {SpinnerContainer, TechnoSpinner} from 'src/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {createOrUpdateTelegrafConfigAsync} from 'src/dataLoaders/actions/dataLoaders'
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {createDashboardsForPlugins as createDashboardsForPluginsAction} from 'src/protos/actions/'

// Constants
import {
  TelegrafConfigCreationSuccess,
  TelegrafConfigCreationError,
} from 'src/shared/copy/notifications'

// Types
import {RemoteDataState, NotificationAction} from 'src/types'
import {AppState} from 'src/types/v2'

interface OwnProps {
  org: string
  children: () => JSX.Element
}

interface StateProps {
  telegrafConfigID: string
}

interface DispatchProps {
  notify: NotificationAction
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
  createDashboardsForPlugins: typeof createDashboardsForPluginsAction
}

type Props = OwnProps & StateProps & DispatchProps

interface State {
  loading: RemoteDataState
}

@ErrorHandling
export class CreateOrUpdateConfig extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {loading: RemoteDataState.NotStarted}
  }

  public async componentDidMount() {
    const {
      onSaveTelegrafConfig,
      notify,
      createDashboardsForPlugins,
      telegrafConfigID,
    } = this.props

    this.setState({loading: RemoteDataState.Loading})

    try {
      await onSaveTelegrafConfig()
      notify(TelegrafConfigCreationSuccess)

      if (!telegrafConfigID) {
        await createDashboardsForPlugins()
      }

      this.setState({loading: RemoteDataState.Done})
    } catch (error) {
      notify(TelegrafConfigCreationError)
      this.setState({loading: RemoteDataState.Error})
    }
  }

  public render() {
    return (
      <SpinnerContainer
        loading={this.state.loading}
        spinnerComponent={<TechnoSpinner />}
      >
        {this.props.children()}
      </SpinnerContainer>
    )
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {telegrafConfigID},
  },
}: AppState): StateProps => {
  return {telegrafConfigID}
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  onSaveTelegrafConfig: createOrUpdateTelegrafConfigAsync,
  createDashboardsForPlugins: createDashboardsForPluginsAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(CreateOrUpdateConfig)
