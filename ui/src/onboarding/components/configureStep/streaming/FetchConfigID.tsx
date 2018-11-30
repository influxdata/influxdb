// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Spinner} from 'src/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Apis
import {getTelegrafConfigs} from 'src/onboarding/apis/index'

// types
import {RemoteDataState} from 'src/types'

export interface Props {
  org: string
  children: (configID: string) => JSX.Element
}

interface State {
  loading: RemoteDataState
  configID?: string
}

@ErrorHandling
class FetchConfigID extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {loading: RemoteDataState.NotStarted}
  }

  public async componentDidMount() {
    const {org} = this.props

    this.setState({loading: RemoteDataState.Loading})
    const telegrafConfigs = await getTelegrafConfigs(org)
    const configID = _.get(telegrafConfigs, '0.id', '')
    this.setState({configID, loading: RemoteDataState.Done})
  }

  public render() {
    return (
      <Spinner loading={this.state.loading}>
        {this.props.children(this.state.configID)}
      </Spinner>
    )
  }
}

export default FetchConfigID
