// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {withRouter, InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

// APIs
import {getSourceHealth} from 'src/sources/apis/v2'
import {getSourcesAsync} from 'src/shared/actions/sources'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {Source} from 'src/types/v2'
import {Location} from 'history'
import {Notification, NotificationFunc, RemoteDataState} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface State {
  loading: RemoteDataState
}

interface Params {
  sourceID: string
}

interface Props {
  getSources: typeof getSourcesAsync
  sources: Source[]
  children: ReactElement<any>
  params: Params
  router: InjectedRouter
  location: Location
  notify: (message: Notification | NotificationFunc) => void
}

export const SourceContext = React.createContext()
// Acts as a 'router middleware'. The main `App` component is responsible for
// getting the list of data sources, but not every page requires them to function.
// Routes that do require data sources can be nested under this component.
@ErrorHandling
export class CheckSources extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    await this.props.getSources()
    this.setState({loading: RemoteDataState.Done})
  }

  public async componentDidUpdate() {
    const {loading} = this.state
    const {router, sources, notify} = this.props
    const source = this.source
    const defaultSource = sources.find(s => s.default === true)

    const isDoneLoading = loading === RemoteDataState.Done

    if (isDoneLoading && !source) {
      if (defaultSource) {
        return router.push(`${this.path}?sourceID=${defaultSource.id}`)
      }

      if (sources[0]) {
        return router.push(`${this.path}?sourceID=${sources[0].id}`)
      }

      return router.push(`/sources/new?redirectPath=${this.path}`)
    }

    if (isDoneLoading) {
      try {
        await getSourceHealth(source.links.health)
      } catch (error) {
        notify(copy.sourceNoLongerAvailable(source.name))
      }
    }
  }

  public render() {
    const source = this.source
    if (this.isLoading || !source) {
      return <div className="page-spinner" />
    }

    return (
      <SourceContext.Provider value={source}>
        {this.props.children &&
          React.cloneElement(this.props.children, {...this.props, source})}
      </SourceContext.Provider>
    )
  }

  private get path(): string {
    const {location} = this.props

    if (this.isRoot) {
      return `/status`
    }

    return `${location.pathname}`
  }

  private get isRoot(): boolean {
    const {
      location: {pathname},
    } = this.props

    return pathname === '' || pathname === '/'
  }

  private get isLoading(): boolean {
    const {loading} = this.state
    return (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.NotStarted
    )
  }

  private get source(): Source {
    const {location, sources} = this.props
    return sources.find(s => s.id === location.query.sourceID)
  }
}

const mstp = ({sources}) => ({
  sources,
})

const mdtp = {
  getSources: getSourcesAsync,
  notify: notifyAction,
}

export default connect(mstp, mdtp)(withRouter(CheckSources))
