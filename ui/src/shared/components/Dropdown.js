import React, {Component, PropTypes} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'
import OnClickOutside from 'shared/components/OnClickOutside'
import FancyScrollbox from 'shared/components/FancyScrollbar'
import {DROPDOWN_MENU_MAX_HEIGHT, DROPDOWN_MENU_ITEM_THRESHOLD} from 'shared/constants/index'

class Dropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
    }

    this.handleClickOutside = ::this.handleClickOutside
    this.handleClick = ::this.handleClick
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

  handleClick(e) {
    this.toggleMenu(e)
    if (this.props.onClick) {
      this.props.onClick(e)
    }
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

  renderShortMenu() {
    const {actions, addNew, items, menuWidth, menuLabel} = this.props
    return (
      <ul className="dropdown-menu" style={{width: menuWidth}}>
        {menuLabel
          ? <li className="dropdown-header">{menuLabel}</li>
          : null
        }
        {items.map((item, i) => {
          if (item.text === 'SEPARATOR') {
            return <li key={i} role="separator" className="divider" />
          }
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
          ? <li className="dropdown-item">
              <Link to={addNew.url}>
                {addNew.text}
              </Link>
            </li>
          : null}
      </ul>
    )
  }

  renderLongMenu() {
    const {actions, addNew, items, menuWidth, menuLabel} = this.props
    return (
      <ul className="dropdown-menu" style={{width: menuWidth, height: DROPDOWN_MENU_MAX_HEIGHT}}>
        <FancyScrollbox autoHide={false}>
          {menuLabel
            ? <li className="dropdown-header">{menuLabel}</li>
            : null
          }
          {items.map((item, i) => {
            if (item.text === 'SEPARATOR') {
              return <li key={i} role="separator" className="divider" />
            }
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
        </FancyScrollbox>
      </ul>
    )
  }

  render() {
    const {
      items,
      selected,
      className,
      iconName,
      buttonSize,
      buttonColor,
    } = this.props
    const {isOpen} = this.state

    return (
      <div
        onClick={this.handleClick}
        className={classnames(`dropdown ${className}`, {open: isOpen})}
      >
        <div className={`btn dropdown-toggle ${buttonSize} ${buttonColor}`}>
          {iconName
            ? <span className={classnames('icon', {[iconName]: true})} />
            : null}
          <span className="dropdown-selected">{selected}</span>
          <span className="caret" />
        </div>
        {(isOpen && items.length < DROPDOWN_MENU_ITEM_THRESHOLD)
          ? this.renderShortMenu()
          : null}
        {(isOpen && items.length >= DROPDOWN_MENU_ITEM_THRESHOLD)
          ? this.renderLongMenu()
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
  menuLabel: string,
}

export default OnClickOutside(Dropdown)
