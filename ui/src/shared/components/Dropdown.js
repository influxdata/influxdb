import React, {Component, PropTypes} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'

import OnClickOutside from 'shared/components/OnClickOutside'

class Dropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
    }

    this.handleClickOutside = ::this.handleClickOutside
    this.handleSelection = ::this.handleSelection
    this.toggleMenu = ::this.toggleMenu
    this.handleAction = ::this.handleAction
  }

  static defaultProps = {
    actions: [],
  }

  handleClickOutside() {
    this.setState({isOpen: false})
  }

  handleSelection(item) {
    this.toggleMenu()
    this.props.onChoose(item)
  }

  toggleMenu(e) {
    if (e) {
      e.stopPropagation()
    }
    this.setState({isOpen: !this.state.isOpen})
  }

  handleAction(e, action, item) {
    e.stopPropagation()
    action.handler(item)
  }

  render() {
    const {items, selected, className, iconName, actions, addNew} = this.props
    const {isOpen} = this.state

    return (
      <div onClick={this.toggleMenu} className={`dropdown ${className}`}>
        <div className="btn btn-sm btn-info dropdown-toggle">
          {iconName ? <span className={classnames("icon", {[iconName]: true})}></span> : null}
          <span className="dropdown-selected">{selected}</span>
          <span className="caret" />
        </div>
        {isOpen ?
          <ul className="dropdown-menu show">
            {items.map((item, i) => {
              return (
                <li className="dropdown-item" key={i}>
                  <a href="#" onClick={() => this.handleSelection(item)}>
                    {item.text}
                  </a>
                  <div className="dropdown-item__actions">
                    {actions.map((action) => {
                      return (
                        <button key={action.text} className="dropdown-item__action" onClick={(e) => this.handleAction(e, action, item)}>
                          <span title={action.text} className={`icon ${action.icon}`}></span>
                        </button>
                      )
                    })}
                  </div>
                </li>
              )
            })}
            {
              addNew ?
                <li>
                  <Link to={addNew.url}>
                    {addNew.text}
                  </Link>
                </li> :
                null
            }
          </ul>
          : null}
      </div>
    )
  }
}

const {
  arrayOf,
  shape,
  string,
  func,
} = PropTypes

Dropdown.propTypes = {
  actions: arrayOf(shape({
    icon: string.isRequired,
    text: string.isRequired,
    handler: func.isRequired,
  })),
  items: arrayOf(shape({
    text: string.isRequired,
  })).isRequired,
  onChoose: func.isRequired,
  addNew: shape({
    url: string.isRequired,
    text: string.isRequired,
  }),
  selected: string.isRequired,
  iconName: string,
  className: string,
}

export default OnClickOutside(Dropdown)
