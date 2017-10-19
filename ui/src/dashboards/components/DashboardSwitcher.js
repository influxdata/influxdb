import React, {Component, PropTypes} from 'react'
import {Link} from 'react-router'
import _ from 'lodash'
import classnames from 'classnames'
import OnClickOutside from 'shared/components/OnClickOutside'

class DashboardSwitcher extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
    }
  }

  handleToggleMenu = () => {
    this.setState({isOpen: !this.state.isOpen})
  }

  handleCloseMenu = () => {
    this.setState({isOpen: false})
  }

  handleClickOutside = () => {
    this.setState({isOpen: false})
  }

  render() {
    const {dashboards, currentDashboard, sourceID, hosts, appParam} = this.props
    const {isOpen} = this.state

    let dropdownItems

    if (hosts) {
      dropdownItems = hosts.map((host, i) => {
        return (
          <li className="dropdown-item" key={i}>
            <Link
              to={`/sources/${sourceID}/hosts/${host + appParam}`}
              onClick={this.handleCloseMenu}
            >
              {host}
            </Link>
          </li>
        )
      })
    }

    if (dashboards) {
      dropdownItems = _.sortBy(dashboards, d =>
        d.name.toLowerCase()
      ).map((d, i) =>
        <li
          className={classnames('dropdown-item', {
            active: d.name === currentDashboard,
          })}
          key={i}
        >
          <Link
            to={`/sources/${sourceID}/dashboards/${d.id}`}
            onClick={this.handleCloseMenu}
          >
            {d.name}
          </Link>
        </li>
      )
    }

    return (
      <div
        className={classnames('dropdown dashboard-switcher', {open: isOpen})}
      >
        <button
          className="btn btn-square btn-default btn-sm dropdown-toggle"
          onClick={this.handleToggleMenu}
        >
          <span className="icon dash-f" />
        </button>
        <ul className="dropdown-menu">
          {dropdownItems}
        </ul>
      </div>
    )
  }
}

const {arrayOf, shape, string} = PropTypes

DashboardSwitcher.propTypes = {
  currentDashboard: string.isRequired,
  dashboards: arrayOf(shape({})).isRequired,
  sourceID: string.isRequired,
  hosts: shape({}),
  appParam: string,
}

export default OnClickOutside(DashboardSwitcher)
