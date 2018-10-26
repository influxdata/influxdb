// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import NavMenu from 'src/pageLayout/components/NavMenu'

// Types
import {Source} from 'src/types/v2'
import {IconFont} from 'src/clockface'

// MOCK DATA
import {LeroyJenkins} from 'src/user/mockUserData'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props extends WithRouterProps {
  sources: Source[]
  isHidden: boolean
}

export enum NavItemType {
  Icon = 'icon',
  Avatar = 'avatar',
}

export interface NavItem {
  title: string
  link: string
  type: NavItemType
  icon?: IconFont
  image?: string
  location: string
  highlightWhen: string[]
}

@ErrorHandling
class SideNav extends PureComponent<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {isHidden} = this.props

    if (isHidden) {
      return null
    }

    return <NavMenu navItems={this.NavigationItems} />
  }

  private get NavigationItems(): NavItem[] {
    const {location} = this.props

    return [
      {
        type: NavItemType.Icon,
        title: 'Status',
        link: `/${this.sourceParam}`,
        icon: IconFont.Cubouniform,
        location: location.pathname,
        highlightWhen: ['status'],
      },
      {
        type: NavItemType.Icon,
        title: 'Data Explorer',
        link: `/data-explorer/${this.sourceParam}`,
        icon: IconFont.Capacitor,
        location: location.pathname,
        highlightWhen: ['data-explorer'],
      },
      {
        type: NavItemType.Icon,
        title: 'Dashboards',
        link: `/dashboards/${this.sourceParam}`,
        icon: IconFont.DashJ,
        location: location.pathname,
        highlightWhen: ['dashboards'],
      },
      {
        type: NavItemType.Icon,
        title: 'Logs',
        link: `/logs/${this.sourceParam}`,
        icon: IconFont.Wood,
        location: location.pathname,
        highlightWhen: ['logs'],
      },
      {
        type: NavItemType.Icon,
        title: 'Tasks',
        link: `/tasks/${this.sourceParam}`,
        icon: IconFont.Alerts,
        location: location.pathname,
        highlightWhen: ['tasks'],
      },
      {
        type: NavItemType.Icon,
        title: 'Organizations',
        link: `/organizations/${this.sourceParam}`,
        icon: IconFont.Group,
        location: location.pathname,
        highlightWhen: ['organizations'],
      },
      {
        type: NavItemType.Icon,
        title: 'Configuration',
        link: `/manage-sources/${this.sourceParam}`,
        icon: IconFont.Wrench,
        location: location.pathname,
        highlightWhen: ['manage-sources'],
      },
      {
        type: NavItemType.Avatar,
        title: 'My Profile',
        link: `/user_profile/${this.sourceParam}`,
        image: LeroyJenkins.avatar,
        location: location.pathname,
        highlightWhen: ['user_profile'],
      },
    ]
  }

  private get sourceParam(): string {
    const {location, sources = []} = this.props

    const {query} = location
    const defaultSource = sources.find(s => s.default)
    const id = query.sourceID || _.get(defaultSource, 'id', 0)

    return `?sourceID=${id}`
  }
}

const mapStateToProps = ({
  sources,
  app: {
    ephemeral: {inPresentationMode},
  },
}) => ({
  sources,
  isHidden: inPresentationMode,
})

export default connect(mapStateToProps)(withRouter(SideNav))
