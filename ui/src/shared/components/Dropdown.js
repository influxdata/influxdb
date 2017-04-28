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
    buttonSize: 'btn-sm',
    buttonColor: 'btn-info',
    menuWidth: '100%',
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
    const {
      items,
      selected,
      className,
      iconName,
      actions,
      addNew,
      buttonSize,
      buttonColor,
      menuWidth,
    } = this.props
    const {isOpen} = this.state

    return (
      <div
        onClick={this.toggleMenu}
        className={classnames(`dropdown ${className}`, {open: isOpen})}
      >
        <div className={`btn dropdown-toggle ${buttonSize} ${buttonColor}`}>
          {iconName
            ? <span className={classnames('icon', {[iconName]: true})} />
            : null}
          <span className="dropdown-selected">{selected}</span>
          <span className="caret" />
        </div>
        {isOpen
          ? <ul className="dropdown-menu" style={{width: menuWidth}}>
              {items.map((item, i) => {
                return (
                  <li className="dropdown-item" key={i}>
                    <a href="#" onClick={() => this.handleSelection(item)}>
                      {item.text}
                    </a>
                    {actions.length > 0
                      ? <div className="dropdown-item__actions">
                          {actions.map(action => {
                            return (
                              <button
                                key={action.text}
                                className="dropdown-item__action"
                                onClick={e =>
                                  this.handleAction(e, action, item)}
                              >
                                <span
                                  title={action.text}
                                  className={`icon ${action.icon}`}
                                />
                              </button>
                            )
                          })}
                        </div>
                      : null}
                  </li>
                )
              })}
              {addNew
                ? <li>
                    <Link to={addNew.url}>
                      {addNew.text}
                    </Link>
                  </li>
                : null}
            </ul>
          : null}
      </div>
    )
  }
}

const {arrayOf, shape, string, func} = PropTypes

Dropdown.propTypes = {
  actions: arrayOf(
    shape({
      icon: string.isRequired,
      text: string.isRequired,
      handler: func.isRequired,
    })
  ),
  items: arrayOf(
    shape({
      text: string.isRequired,
    })
  ).isRequired,
  onChoose: func.isRequired,
  onClick: func,
  addNew: shape({
    url: string.isRequired,
    text: string.isRequired,
  }),
  selected: string.isRequired,
  iconName: string,
  className: string,
  buttonSize: string,
  buttonColor: string,
  menuWidth: string,
}

export default OnClickOutside(Dropdown)
