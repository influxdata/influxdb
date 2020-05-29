// Libraries
import {PureComponent} from 'react'

// Types
import {RemoteDataState} from 'src/types'

interface Props<T> {
  fetcher: () => Promise<T>
  children: (resources: T, loading: RemoteDataState) => JSX.Element
}

interface State<T> {
  resources: T
  loading: RemoteDataState
}

export default class ResourceFetcher<T> extends PureComponent<
  Props<T>,
  State<T>
> {
  constructor(props) {
    super(props)
    this.state = {
      resources: null,
      loading: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    const {fetcher} = this.props
    let resources
    try {
      resources = await fetcher()
    } catch (error) {
      console.error(error)
    }

    this.setState({resources, loading: RemoteDataState.Done})
  }

  public render() {
    const {resources, loading} = this.state
    return this.props.children(resources, loading)
  }
}
