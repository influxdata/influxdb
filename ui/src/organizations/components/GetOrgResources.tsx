// Libraries
import {PureComponent} from 'react'

// Types
import {RemoteDataState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props<T> {
  link: string
  fetcher: (link: string) => Promise<T>
  children: (resources: T, loading: RemoteDataState) => JSX.Element
}

interface State<T> {
  resources: T
  loading: RemoteDataState
}

@ErrorHandling
export default class GetOrgResources<T> extends PureComponent<
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
    const {link, fetcher} = this.props
    const resources = await fetcher(link)
    this.setState({resources, loading: RemoteDataState.Done})
  }

  public render() {
    const {resources, loading} = this.state
    return this.props.children(resources, loading)
  }
}
