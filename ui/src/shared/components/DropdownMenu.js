import React from 'react'
import PropTypes from 'prop-types'
import {Link} from 'react-router'

import classnames from 'classnames'
import {DROPDOWN_MENU_MAX_HEIGHT} from 'shared/constants/index'
import FancyScrollbar from 'shared/components/FancyScrollbar'

// AddNewResource is an optional parameter that takes the user to another
// route defined by url prop
const AddNewButton = ({url, text}) => (
  <li className="multi-select--apply">
    <Link className="btn btn-xs btn-default" to={url}>
      {text}
    </Link>
  </li>
)

const DropdownMenu = ({
  items,
  addNew,
  actions,
  selected,
  onAction,
  menuClass,
  menuWidth,
  menuLabel,
  onSelection,
  onHighlight,
  useAutoComplete,
  highlightedItemIndex,
}) => {
  return (
    <ul
      className={classnames('dropdown-menu', {
        'dropdown-menu--no-highlight': useAutoComplete,
        [menuClass]: menuClass,
      })}
      style={{width: menuWidth}}
      data-test="dropdown-ul"
    >
      <FancyScrollbar
        autoHide={false}
        autoHeight={true}
        maxHeight={DROPDOWN_MENU_MAX_HEIGHT}
      >
        {menuLabel ? <li className="dropdown-header">{menuLabel}</li> : null}
        {items.map((item, i) => {
          if (item.text === 'SEPARATOR') {
            return <li key={i} className="dropdown-divider" />
          }

          return (
            <li
              className={classnames('dropdown-item', {
                highlight: i === highlightedItemIndex,
                active: item.text === selected,
              })}
              data-test="dropdown-item"
              key={i}
            >
              <a
                href="#"
                onClick={onSelection(item)}
                onMouseOver={onHighlight(i)}
              >
                {item.text}
              </a>
              {actions && actions.length ? (
                <div className="dropdown-actions">
                  {actions.map(action => {
                    return (
                      <button
                        key={action.text}
                        className="dropdown-action"
                        onClick={onAction(action, item)}
                      >
                        <span
                          title={action.text}
                          className={`icon ${action.icon}`}
                        />
                      </button>
                    )
                  })}
                </div>
              ) : null}
            </li>
          )
        })}
        {addNew && <AddNewButton url={addNew.url} text={addNew.text} />}
      </FancyScrollbar>
    </ul>
  )
}

export const DropdownMenuEmpty = ({useAutoComplete, menuClass}) => (
  <ul
    className={classnames('dropdown-menu', {
      'dropdown-menu--no-highlight': useAutoComplete,
      [menuClass]: menuClass,
    })}
  >
    <li className="dropdown-empty">No matching items</li>
  </ul>
)

const {arrayOf, bool, number, shape, string, func} = PropTypes

AddNewButton.propTypes = {
  url: string,
  text: string,
}

DropdownMenuEmpty.propTypes = {
  useAutoComplete: bool,
  menuClass: string,
}

DropdownMenu.propTypes = {
  onAction: func,
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
  onClick: func,
  addNew: shape({
    url: string.isRequired,
    text: string.isRequired,
  }),
  selected: string.isRequired,
  iconName: string,
  className: string,
  buttonColor: string,
  menuWidth: string,
  menuLabel: string,
  menuClass: string,
  useAutoComplete: bool,
  disabled: bool,
  searchTerm: string,
  onSelection: func,
  onHighlight: func,
  highlightedItemIndex: number,
}

export default DropdownMenu
