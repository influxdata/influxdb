// Libraries
import {PureComponent} from 'react'
import _ from 'lodash'

// APIs
import {client} from 'src/utils/api'

// Types
import {RemoteDataState} from 'src/types'
import {Label} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: (labels: Label[], loading: RemoteDataState) => JSX.Element
}

interface State {
  labels: Label[]
  loading: RemoteDataState
}

@ErrorHandling
export default class GetLabels extends PureComponent<Props, State> {
  public state: State = {labels: null, loading: RemoteDataState.NotStarted}

  public async componentDidMount() {
    const labels = await client.labels.getAll()
    this.setState({
      labels: _.orderBy(labels, ['name']),
      loading: RemoteDataState.Done,
    })
  }

  public render() {
    const {labels, loading} = this.state
    return this.props.children(labels, loading)
  }
}
