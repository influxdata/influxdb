import React, {SFC} from 'react'
import {Link} from 'react-router'

import classnames from 'classnames'
import {DROPDOWN_MENU_MAX_HEIGHT} from 'src/shared/constants/index'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import DropdownMenuItem from 'src/shared/components/DropdownMenuItem'
import {
  OnActionHandler,
  OnSelectionHandler,
  OnHighlightHandler,
} from 'src/shared/components/DropdownMenuItem'

import {DropdownItem, DropdownAction} from 'src/types'

// AddNewResource is an optional parameter that takes the user to another
// route defined by url prop
interface AddNewButtonProps {
  url: string
  text: string
}

const AddNewButton: SFC<AddNewButtonProps> = ({url, text}) => (
  <li className="multi-select--apply">
    <Link className="btn btn-xs btn-default" to={url}>
      {text}
    </Link>
  </li>
)

interface AddNew {
  url: string
  text: string
}

interface Props {
  onAction?: OnActionHandler
  actions: DropdownAction[]
  items: DropdownItem[]
  selected: string
  addNew?: AddNew
  iconName?: string
  buttonColor?: string
  menuWidth?: string
  menuLabel?: string
  menuClass?: string
  useAutoComplete?: boolean
  disabled?: boolean
  searchTerm?: string
  onSelection?: OnSelectionHandler
  onHighlight?: OnHighlightHandler
  highlightedItemIndex?: number
}

const DropdownMenu: SFC<Props> = ({
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
        {items.map((item, i) => (
          <DropdownMenuItem
            item={item}
            actions={actions}
            onAction={onAction}
            highlightedItemIndex={highlightedItemIndex}
            onHighlight={onHighlight}
            selected={selected}
            onSelection={onSelection}
            index={i}
            key={i}
          />
        ))}
        {addNew && <AddNewButton url={addNew.url} text={addNew.text} />}
      </FancyScrollbar>
    </ul>
  )
}

interface DropdownMenuEmptyProps {
  useAutoComplete?: boolean
  menuClass: string
}

export const DropdownMenuEmpty: SFC<DropdownMenuEmptyProps> = ({
  useAutoComplete,
  menuClass,
}) => (
  <ul
    className={classnames('dropdown-menu', {
      'dropdown-menu--no-highlight': useAutoComplete,
      [menuClass]: menuClass,
    })}
  >
    <li className="dropdown-empty">No matching items</li>
  </ul>
)

export default DropdownMenu
