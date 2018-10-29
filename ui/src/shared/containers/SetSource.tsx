import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, InjectedRouter, WithRouterProps} from 'react-router'
import {Location} from 'history'

import {Source} from 'src/types/v2'
import {Links} from 'src/types/v2/links'
import {notify} from 'src/shared/actions/notifications'
import {Notification, NotificationFunc} from 'src/types'

import {setSource, resetSource} from 'src/shared/actions/v2/source'

import {getSourceHealth} from 'src/sources/apis/v2'
import * as copy from 'src/shared/copy/notifications'

interface PassedInProps extends WithRouterProps {
  router: InjectedRouter
  children: React.ReactElement<any>
  location: Location
}

interface ConnectStateProps {
  sources: Source[]
  links: Links
}

interface ConnectDispatchProps {
  setSource: typeof setSource
  resetSource: typeof resetSource
  notify: (message: Notification | NotificationFunc) => void
}

type Props = ConnectStateProps & ConnectDispatchProps & PassedInProps

export const SourceContext = React.createContext({})

class SetSource extends PureComponent<Props> {
  public render() {
    return this.props.children && React.cloneElement(this.props.children)
  }

  public componentDidMount() {
    const source = this.source

    if (source) {
      this.props.setSource(source)
    }
  }

  public async componentDidUpdate() {
    const {router, sources} = this.props
    const source = this.source
    const defaultSource = sources.find(s => s.default === true)

    if (this.isRoot) {
      return router.push(`${this.rootPath}`)
    }

    if (!source) {
      if (defaultSource) {
        return router.push(`${this.path}?sourceID=${defaultSource.id}`)
      }

      if (sources[0]) {
        return router.push(`${this.path}?sourceID=${sources[0].id}`)
      }

      return router.push(`/sources/new?redirectPath=${this.path}`)
    }

    try {
      await getSourceHealth(source.links.health)
      this.props.setSource(source)
    } catch (error) {
      this.props.notify(copy.sourceNoLongerAvailable(source.name))
      this.props.resetSource()
    }
  }

  private get path(): string {
    const {location} = this.props

    if (this.isRoot) {
      return this.rootPath
    }

    return `${location.pathname}`
  }

  private get rootPath(): string {
    const {links, location} = this.props
    if (links.defaultDashboard) {
      const split = links.defaultDashboard.split('/')
      const id = split[split.length - 1]
      return `/dashboards/${id}${location.search}`
    }

    return `/dashboards`
  }

  private get isRoot(): boolean {
    const {
      location: {pathname},
    } = this.props

    return pathname === '' || pathname === '/'
  }

  private get source(): Source {
    const {location, sources} = this.props

    return sources.find(s => s.id === location.query.sourceID)
  }
}

const mstp = ({sources, links}) => ({
  links,
  sources,
})

const mdtp = {
  setSource,
  resetSource,
  notify,
}

export default connect<ConnectStateProps, ConnectDispatchProps, PassedInProps>(
  mstp,
  mdtp
)(withRouter(SetSource))
