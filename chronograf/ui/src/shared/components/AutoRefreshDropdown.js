import React, {Component} from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'
import OnClickOutside from 'shared/components/OnClickOutside'

import autoRefreshItems from 'shared/data/autoRefreshes'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class AutoRefreshDropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
    }
  }

  findAutoRefreshItem(milliseconds) {
    return autoRefreshItems.find(values => values.milliseconds === milliseconds)
  }

  handleClickOutside() {
    this.setState({isOpen: false})
  }

  handleSelection = milliseconds => () => {
    this.props.onChoose(milliseconds)
    this.setState({isOpen: false})
  }

  toggleMenu = () => this.setState({isOpen: !this.state.isOpen})

  render() {
    const {selected, onManualRefresh} = this.props
    const {isOpen} = this.state
    const {milliseconds, inputValue} = this.findAutoRefreshItem(selected)

    return (
      <div
        className={classnames('autorefresh-dropdown', {
          paused: +milliseconds === 0,
        })}
      >
        <div className={classnames('dropdown dropdown-120', {open: isOpen})}>
          <div
            className="btn btn-sm btn-default dropdown-toggle"
            onClick={this.toggleMenu}
          >
            <span
              className={classnames(
                'icon',
                +milliseconds > 0 ? 'refresh' : 'pause'
              )}
            />
            <span className="dropdown-selected">{inputValue}</span>
            <span className="caret" />
          </div>
          <ul className="dropdown-menu">
            <li className="dropdown-header">AutoRefresh</li>
            {autoRefreshItems.map(item => (
              <li className="dropdown-item" key={item.menuOption}>
                <a href="#" onClick={this.handleSelection(item.milliseconds)}>
                  {item.menuOption}
                </a>
              </li>
            ))}
          </ul>
        </div>
        {+milliseconds === 0 ? (
          <div
            className="btn btn-sm btn-default btn-square"
            onClick={onManualRefresh}
          >
            <span className="icon refresh" />
          </div>
        ) : null}
      </div>
    )
  }
}

const {number, func} = PropTypes

AutoRefreshDropdown.propTypes = {
  selected: number.isRequired,
  onChoose: func.isRequired,
  onManualRefresh: func,
}

export default OnClickOutside(AutoRefreshDropdown)
