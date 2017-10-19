import React, {Component, PropTypes} from 'react'
import {Link} from 'react-router'
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
    const {dashboards, currentDashboard, sourceID} = this.props
    const {isOpen} = this.state

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
          {dashboards.map((d, i) =>
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
          )}
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
}

export default OnClickOutside(DashboardSwitcher)
