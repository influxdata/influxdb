// Libraries
import {PureComponent} from 'react'

// APIs
import {getLabels} from 'src/configuration/apis'

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
    const labels = await getLabels()
    this.setState({labels, loading: RemoteDataState.Done})
  }

  public render() {
    const {labels, loading} = this.state
    return this.props.children(labels, loading)
  }
}
