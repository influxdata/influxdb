// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import NavMenu from 'src/side_nav/components/NavMenu'

// Types
import {Source} from 'src/types/v2'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props extends WithRouterProps {
  sources: Source[]
  isHidden: boolean
}

interface NavItem {
  title: string
  link: string
  icon: string
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
        title: 'Status',
        link: `/status/${this.sourceParam}`,
        icon: 'cubo-uniform',
        location: location.pathname,
        highlightWhen: ['status'],
      },
      {
        title: 'Flux Builder',
        link: `/delorean/${this.sourceParam}`,
        icon: 'capacitor2',
        location: location.pathname,
        highlightWhen: ['delorean'],
      },
      {
        title: 'Dashboards',
        link: `/dashboards/${this.sourceParam}`,
        icon: 'dash-j',
        location: location.pathname,
        highlightWhen: ['dashboards'],
      },
      {
        title: 'Configuration',
        link: `/manage-sources/${this.sourceParam}`,
        icon: 'wrench',
        location: location.pathname,
        highlightWhen: ['manage-sources'],
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
