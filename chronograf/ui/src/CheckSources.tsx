// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {withRouter, InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

// APIs
import {getSourceHealth} from 'src/sources/apis'
import {getSourcesAsync} from 'src/shared/actions/sources'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {DEFAULT_HOME_PAGE} from 'src/shared/constants'
import * as copy from 'src/shared/copy/notifications'

// Types
import {
  Source,
  Notification,
  NotificationFunc,
  RemoteDataState,
} from 'src/types'
import {Location} from 'history'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface State {
  loading: RemoteDataState
}

interface Params {
  sourceID: string
}

interface Props {
  getSources: () => void
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
    const {router, location, sources, notify} = this.props
    const source = this.source
    const defaultSource = sources.find(s => s.default === true)

    const rest = location.pathname.match(/\/sources\/\d+?\/(.+)/)
    const restString = rest === null ? DEFAULT_HOME_PAGE : rest[1]

    const isDoneLoading = loading === RemoteDataState.Done

    if (isDoneLoading && !source) {
      if (defaultSource) {
        return router.push(`/sources/${defaultSource.id}/${restString}`)
      }

      if (sources[0]) {
        return router.push(`/sources/${sources[0].id}/${restString}`)
      }

      return router.push(`/sources/new?redirectPath=${location.pathname}`)
    }

    if (isDoneLoading) {
      try {
        await getSourceHealth(source.links.health)
      } catch (error) {
        notify(copy.notifySourceNoLongerAvailable(source.name))
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

  private get isLoading(): boolean {
    const {loading} = this.state
    return (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.NotStarted
    )
  }

  private get source(): Source {
    const {params, sources} = this.props
    return sources.find(s => s.id === params.sourceID)
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
