// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import NavMenu from 'src/pageLayout/components/NavMenu'

// Utils
import {getSources} from 'src/sources/selectors'

// Types
import {Source, MeState, AppState} from 'src/types/v2'
import {IconFont} from 'src/clockface'

// Styles
import '../PageLayout.scss'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props extends WithRouterProps {
  sources: Source[]
  isHidden: boolean
  me: MeState
}

@ErrorHandling
class SideNav extends PureComponent<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {isHidden, me} = this.props

    if (isHidden) {
      return null
    }

    return (
      <NavMenu>
        <NavMenu.Item
          title={me.name}
          link="/me"
          icon={IconFont.CuboNav}
          location={location.pathname}
          highlightWhen={['me', 'account']}
        >
          <NavMenu.SubItem
            title="Logout"
            link="/logout"
            location={location.pathname}
            highlightWhen={[]}
          />
        </NavMenu.Item>
        <NavMenu.Item
          title="Data Explorer"
          link="/data-explorer"
          icon={IconFont.GraphLine}
          location={location.pathname}
          highlightWhen={['data-explorer']}
        />
        <NavMenu.Item
          title="Dashboards"
          link="/dashboards"
          icon={IconFont.Dashboards}
          location={location.pathname}
          highlightWhen={['dashboards']}
        />
        <NavMenu.Item
          title="Tasks"
          link="/tasks"
          icon={IconFont.Calendar}
          location={location.pathname}
          highlightWhen={['tasks']}
        />
        <NavMenu.Item
          title="Organizations"
          link="/organizations"
          icon={IconFont.UsersDuo}
          location={location.pathname}
          highlightWhen={['organizations']}
        />
        <NavMenu.Item
          title="Configuration"
          link="/configuration/labels_tab"
          icon={IconFont.Wrench}
          location={location.pathname}
          highlightWhen={['configuration']}
        >
          <NavMenu.SubItem
            title="Labels"
            link="/configuration/labels_tab"
            location={location.pathname}
            highlightWhen={['labels']}
          />
          <NavMenu.SubItem
            title="My Settings"
            link="/configuration/settings_tab"
            location={location.pathname}
            highlightWhen={['settings']}
          />
          <NavMenu.SubItem
            title="My Tokens"
            link="/configuration/tokens_tab"
            location={location.pathname}
            highlightWhen={['tokens']}
          />
        </NavMenu.Item>
      </NavMenu>
    )
  }
}

const mstp = (state: AppState) => {
  const isHidden = state.app.ephemeral.inPresentationMode
  const sources = getSources(state)
  const {me} = state

  return {sources, isHidden, me}
}

export default connect(mstp)(withRouter(SideNav))
