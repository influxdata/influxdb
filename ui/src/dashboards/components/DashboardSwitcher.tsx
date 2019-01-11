import React, {PureComponent} from 'react'
import {Link, withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'
import classnames from 'classnames'

import {ClickOutside} from 'src/shared/components/ClickOutside'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

import {ErrorHandling} from 'src/shared/decorators/errors'

import {DROPDOWN_MENU_MAX_HEIGHT} from 'src/shared/constants/index'
import {DashboardSwitcherLinks} from 'src/types/v2/dashboards'

interface Props {
  dashboardLinks: DashboardSwitcherLinks
}

interface State {
  isOpen: boolean
}

@ErrorHandling
class DashboardSwitcher extends PureComponent<Props & WithRouterProps, State> {
  constructor(props) {
    super(props)

    this.state = {isOpen: false}
  }

  public render() {
    const {isOpen} = this.state

    const openClass = isOpen ? 'open' : ''

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div className={`dropdown dashboard-switcher ${openClass}`}>
          <button
            className="btn btn-square btn-default btn-sm dropdown-toggle"
            onClick={this.handleToggleMenu}
          >
            <span className="icon dash-h" />
          </button>
          <ul className="dropdown-menu">
            <FancyScrollbar
              autoHeight={true}
              maxHeight={DROPDOWN_MENU_MAX_HEIGHT}
            >
              {this.links}
            </FancyScrollbar>
          </ul>
        </div>
      </ClickOutside>
    )
  }

  public handleClickOutside = () => {
    this.setState({isOpen: false})
  }

  private handleToggleMenu = () => {
    this.setState({isOpen: !this.state.isOpen})
  }

  private handleCloseMenu = () => {
    this.setState({isOpen: false})
  }

  private get links(): JSX.Element[] {
    const {links, active} = this.props.dashboardLinks

    return _.sortBy(links, ['text', 'key']).map(link => {
      return (
        <li
          key={link.key}
          className={classnames('dropdown-item', {
            active: link === active,
          })}
        >
          <Link
            to={{pathname: link.to, query: this.sourceID}}
            onClick={this.handleCloseMenu}
          >
            {link.text}
          </Link>
        </li>
      )
    })
  }

  private get sourceID(): {sourceID: string} | {} {
    const {query} = this.props.location
    const {sourceID} = query
    if (query.sourceID) {
      return {sourceID}
    }

    return {}
  }
}

export default withRouter(DashboardSwitcher)
