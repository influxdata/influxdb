import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, InjectedRouter, WithRouterProps} from 'react-router'
import {Location} from 'history'

import {Source} from 'src/types/v2'
import {Links} from 'src/types/v2/links'
import {Notification, NotificationFunc} from 'src/types'

import {getSourceHealth} from 'src/sources/apis/v2'
import * as copy from 'src/shared/copy/notifications'

interface PassedInProps extends WithRouterProps {
  router: InjectedRouter
  children: React.ReactElement<any>
  location: Location
  notify: (message: Notification | NotificationFunc) => void
}

interface ConnectStateProps {
  sources: Source[]
  links: Links
}

type Props = ConnectStateProps & PassedInProps

export const SourceContext = React.createContext({})

class SetSource extends PureComponent<Props> {
  public render() {
    const source = this.source

    return (
      <SourceContext.Provider value={source}>
        {this.props.children &&
          React.cloneElement(this.props.children, {...this.props, source})}
      </SourceContext.Provider>
    )
  }

  public async componentDidUpdate() {
    const {router, sources, notify} = this.props
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
    } catch (error) {
      notify(copy.sourceNoLongerAvailable(source.name))
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

export default connect<ConnectStateProps, {}, PassedInProps>(mstp)(
  withRouter(SetSource)
)
