// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import NavMenu from 'src/pageLayout/components/NavMenu'

// Utils
import {getSources} from 'src/sources/selectors'

// Types
import {Source, AppState} from 'src/types/v2'
import {IconFont} from 'src/clockface'

// Styles
import '../PageLayout.scss'

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
        title: 'My Profile',
        link: '/me',
        icon: IconFont.Cubouniform,
        location: location.pathname,
        highlightWhen: ['user_profile'],
      },
      {
        type: NavItemType.Icon,
        title: 'Status',
        link: '/',
        icon: IconFont.Cubouniform,
        location: location.pathname,
        highlightWhen: ['status'],
      },
      {
        type: NavItemType.Icon,
        title: 'Data Explorer',
        link: '/data-explorer',
        icon: IconFont.Capacitor,
        location: location.pathname,
        highlightWhen: ['data-explorer'],
      },
      {
        type: NavItemType.Icon,
        title: 'Dashboards',
        link: '/dashboards',
        icon: IconFont.DashJ,
        location: location.pathname,
        highlightWhen: ['dashboards'],
      },
      {
        type: NavItemType.Icon,
        title: 'Tasks',
        link: '/tasks',
        icon: IconFont.Alerts,
        location: location.pathname,
        highlightWhen: ['tasks'],
      },
      {
        type: NavItemType.Icon,
        title: 'Organizations',
        link: '/organizations',
        icon: IconFont.Group,
        location: location.pathname,
        highlightWhen: ['organizations'],
      },
      {
        type: NavItemType.Icon,
        title: 'Sources',
        link: '/sources',
        icon: IconFont.Wrench,
        location: location.pathname,
        highlightWhen: ['sources'],
      },
    ]
  }
}

const mstp = (state: AppState) => {
  const isHidden = state.app.ephemeral.inPresentationMode
  const sources = getSources(state)

  return {sources, isHidden}
}

export default connect(mstp)(withRouter(SideNav))
