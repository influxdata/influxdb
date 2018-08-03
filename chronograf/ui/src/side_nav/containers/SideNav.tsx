// Libraries
import React, {PureComponent} from 'react'
import {withRouter, Link} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {NavBlock, NavHeader} from 'src/side_nav/components/NavItems'

// Constants
import {DEFAULT_HOME_PAGE} from 'src/shared/constants'

// Types
import {Params, Location} from 'src/types/sideNav'
import {Source} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  sources: Source[]
  params: Params
  location: Location
  isHidden: boolean
}

@ErrorHandling
class SideNav extends PureComponent<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {
      params: {sourceID},
      location: {pathname: location},
      isHidden,
      sources = [],
    } = this.props

    const defaultSource = sources.find(s => s.default)
    const id = sourceID || _.get(defaultSource, 'id', 0)

    const sourcePrefix = `/sources/${id}`
    const isDefaultPage = location.split('/').includes(DEFAULT_HOME_PAGE)

    return isHidden ? null : (
      <nav className="sidebar">
        <div
          className={isDefaultPage ? 'sidebar--item active' : 'sidebar--item'}
        >
          <Link
            to={`${sourcePrefix}/${DEFAULT_HOME_PAGE}`}
            className="sidebar--square sidebar--logo"
          >
            <span className="sidebar--icon icon cubo-uniform" />
          </Link>
        </div>
        <NavBlock
          highlightWhen={['delorean']}
          icon="capacitor2"
          link={`${sourcePrefix}/delorean`}
          location={location}
        >
          <NavHeader link={`${sourcePrefix}/delorean`} title="Flux Editor" />
        </NavBlock>
        <NavBlock
          highlightWhen={['dashboards']}
          icon="dash-j"
          link={`${sourcePrefix}/dashboards`}
          location={location}
        >
          <NavHeader link={`${sourcePrefix}/dashboards`} title="Dashboards" />
        </NavBlock>
        <NavBlock
          highlightWhen={['logs']}
          icon="wood"
          link="/logs"
          location={location}
        >
          <NavHeader link={'/logs'} title="Log Viewer" />
        </NavBlock>
        <NavBlock
          highlightWhen={['manage-sources', 'kapacitors']}
          icon="wrench"
          link={`${sourcePrefix}/manage-sources`}
          location={location}
        >
          <NavHeader
            link={`${sourcePrefix}/manage-sources`}
            title="Configuration"
          />
        </NavBlock>
      </nav>
    )
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
