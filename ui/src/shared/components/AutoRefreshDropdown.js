import React, {PropTypes} from 'react'
import classnames from 'classnames'
import OnClickOutside from 'shared/components/OnClickOutside'

import autoRefreshItems from 'hson!shared/data/autoRefreshes.hson'

const {number, func} = PropTypes

const AutoRefreshDropdown = React.createClass({
  autobind: false,

  propTypes: {
    selected: number.isRequired,
    onChoose: func.isRequired,
  },

  getInitialState() {
    return {
      isOpen: false,
    }
  },

  findAutoRefreshItem(milliseconds) {
    return autoRefreshItems.find(values => values.milliseconds === milliseconds)
  },

  handleClickOutside() {
    this.setState({isOpen: false})
  },

  handleSelection(milliseconds) {
    this.props.onChoose(milliseconds)
    this.setState({isOpen: false})
  },

  toggleMenu() {
    this.setState({isOpen: !this.state.isOpen})
  },

  render() {
    const self = this
    const {selected} = self.props
    const {isOpen} = self.state
    const {milliseconds, inputValue} = this.findAutoRefreshItem(selected)

    return (
      <div className={classnames('dropdown dropdown-160', {open: isOpen})}>
        <div
          className="btn btn-sm btn-default dropdown-toggle"
          onClick={() => self.toggleMenu()}
        >
          <span
            className={classnames(
              'icon',
              +milliseconds > 0 ? 'refresh' : 'pause'
            )}
          />
          <span className="dropdown-selected">
            {inputValue}
          </span>
          <span className="caret" />
        </div>
        <ul className="dropdown-menu">
          <li className="dropdown-header">AutoRefresh Interval</li>
          {autoRefreshItems.map(item => {
            return (
              <li className="dropdown-item" key={item.menuOption}>
                <a
                  href="#"
                  onClick={() => self.handleSelection(item.milliseconds)}
                >
                  {item.menuOption}
                </a>
              </li>
            )
          })}
        </ul>
      </div>
    )
  },
})

export default OnClickOutside(AutoRefreshDropdown)
