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
    const {activeDashboard, names} = this.props
    const {isOpen} = this.state
    const sorted = _.sortBy(names, ({name}) => name.toLowerCase())

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
          {sorted.map(({name, link}) =>
            <NameLink
              key={link}
              name={name}
              link={link}
              activeName={activeDashboard}
              onClose={this.handleCloseMenu}
            />
          )}
        </ul>
      </div>
    )
  }
}

const NameLink = ({name, link, activeName, onClose}) =>
  <li
    className={classnames('dropdown-item', {
      active: name === activeName,
    })}
  >
    <Link to={link} onClick={onClose}>
      {name}
    </Link>
  </li>

const {arrayOf, func, shape, string} = PropTypes

DashboardSwitcher.propTypes = {
  activeDashboard: string.isRequired,
  names: arrayOf(
    shape({
      link: string.isRequired,
      name: string.isRequired,
    })
  ).isRequired,
}

NameLink.propTypes = {
  name: string.isRequired,
  link: string.isRequired,
  activeName: string.isRequired,
  onClose: func.isRequired,
}

export default OnClickOutside(DashboardSwitcher)
