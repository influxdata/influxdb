// Libraries
import {PureComponent} from 'react'

// Types
import {RemoteDataState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Organization} from 'src/api'

interface Props<T> {
  organization: Organization
  fetcher: (org: Organization) => Promise<T>
  children: (
    resources: T,
    loading: RemoteDataState,
    fetch?: () => void
  ) => JSX.Element
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
  constructor(props: Props<T>) {
    super(props)
    this.state = {
      resources: null,
      loading: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    this.fetchResourcesForOrg()
  }

  public async componentDidUpdate(prevProps: Props<T>) {
    if (this.props.organization !== prevProps.organization) {
      this.fetchResourcesForOrg()
    }
  }

  public render() {
    const {resources, loading} = this.state
    return this.props.children(resources, loading, this.fetchResourcesForOrg)
  }

  public fetchResourcesForOrg = async () => {
    const {fetcher, organization} = this.props
    if (organization) {
      const resources = await fetcher(organization)
      this.setState({resources, loading: RemoteDataState.Done})
    }
  }
}
