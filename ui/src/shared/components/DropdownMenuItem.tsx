import React, {SFC, MouseEvent} from 'react'

import _ from 'lodash'
import classnames from 'classnames'
import {DropdownAction, DropdownItem} from 'src/types'

export type OnSelectionHandler = (
  item: DropdownItem
) => (e: MouseEvent<HTMLAnchorElement>) => void

export type OnHighlightHandler = (
  key: number
) => (e: MouseEvent<HTMLAnchorElement>) => void

export type OnActionHandler = (
  action: DropdownAction,
  item: DropdownItem
) => (e: MouseEvent<HTMLElement>) => void

interface ItemProps {
  index: number
  selected: string
  item: DropdownItem
  highlightedItemIndex?: number
  onSelection?: OnSelectionHandler
  onHighlight?: OnHighlightHandler
  actions?: DropdownAction[]
  onAction?: OnActionHandler
}

const DropdownMenuItem: SFC<ItemProps> = ({
  item,
  highlightedItemIndex,
  onSelection,
  onHighlight,
  actions,
  onAction,
  selected,
  index,
}) => {
  if (_.isString(item)) {
    item = {text: item}
  }

  if (item.text === 'SEPARATOR') {
    return <li className="dropdown-divider" />
  }

  return (
    <li
      className={classnames('dropdown-item', {
        highlight: index === highlightedItemIndex,
        active: item.text === selected,
      })}
      data-test="dropdown-item"
    >
      <a href="#" onClick={onSelection(item)} onMouseOver={onHighlight(index)}>
        {item.text}
      </a>
      {actions &&
        !!actions.length && (
          <div className="dropdown-actions">
            {actions.map(action => {
              return (
                <button
                  key={action.text}
                  className="dropdown-action"
                  onClick={onAction(action, item)}
                >
                  <span title={action.text} className={`icon ${action.icon}`} />
                </button>
              )
            })}
          </div>
        )}
    </li>
  )
}

export default DropdownMenuItem
