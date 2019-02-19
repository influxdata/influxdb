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
  fetcher: (org: Organization) => Promise<T[]>
  children: (
    resources: T[],
    loading: RemoteDataState,
    fetch?: () => void
  ) => JSX.Element
}

interface State<T> {
  resources: T[]
  loading: RemoteDataState
}

interface DefaultProps<T> {
  orderBy?: {
    keys: Array<keyof T>
    orders?: string[]
  }
}

type Props<T> = DefaultProps<T> & PassedProps<T>

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
        resources: this.order(resources),
        loading: RemoteDataState.Done,
      })
    }
  }

  private order(resources: T[]): T[] {
    const {orderBy} = this.props

    const defaultKeys = this.extractDefaultKeys(resources)

    if (orderBy) {
      return _.orderBy(resources, orderBy.keys, orderBy.orders)
    } else if (defaultKeys.length !== 0) {
      return _.orderBy(resources, defaultKeys)
    } else {
      return resources
    }
  }

  private extractDefaultKeys(resources: T[]): Array<keyof T> {
    return this.hasKeyOf(resources, DEFAULT_SORT_KEY) ? [DEFAULT_SORT_KEY] : []
  }

  private hasKeyOf(
    resources: T[],
    key: string | number | symbol
  ): key is keyof T {
    const resource = _.get(resources, '0', null)
    // gaurd against null and primitive types
    const isObject = !!resource && typeof resource === 'object'

    return isObject && _.hasIn(resource, key)
  }
}
