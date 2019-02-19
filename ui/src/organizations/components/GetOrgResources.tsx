// Libraries
import {PureComponent} from 'react'
import _ from 'lodash'

// Types
import {RemoteDataState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Organization} from '@influxdata/influx'

interface PassedProps<T> {
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

interface DefaultProps<T> {
  orderBy?: {
    keys: Array<keyof Unpack<T>>
    orders?: string[]
  }
}

type Props<T> = DefaultProps<T> & PassedProps<T>

type Unpack<T> = T extends Array<infer U> ? U : never

const DEFAULT_SORT_KEY = 'name'

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
      this.setState({
        resources: this.order(resources) as T,
        loading: RemoteDataState.Done,
      })
    }
  }

  // Todo: improve typing for unpacking array
  private order(resources: T | Array<Unpack<T>>): T | Array<Unpack<T>> {
    const {orderBy} = this.props
    if (!this.isArray(resources)) {
      return resources
    } else {
      const defaultKeys = this.extractDefaultKeys(resources)
      if (orderBy) {
        return _.orderBy(resources, orderBy.keys, orderBy.orders)
      } else if (defaultKeys.length !== 0) {
        return _.orderBy(resources, defaultKeys)
      } else {
        return resources
      }
    }
  }

  private extractDefaultKeys(
    resources: Array<Unpack<T>>
  ): Array<keyof Unpack<T>> {
    return this.hasKeyOf(resources, DEFAULT_SORT_KEY) ? [DEFAULT_SORT_KEY] : []
  }

  private hasKeyOf(
    resources: Array<Unpack<T>>,
    key: string | number | symbol
  ): key is keyof Unpack<T> {
    return key in resources[0]
  }

  private isArray(
    resources: T | Array<Unpack<T>>
  ): resources is Array<Unpack<T>> {
    return resources instanceof Array
  }
}
