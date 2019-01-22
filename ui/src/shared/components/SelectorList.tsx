import React, {SFC} from 'react'

import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

import 'src/shared/components/SelectorList.scss'

interface Props {
  items: string[]
  selectedItems: string[]
  onSelectItem: (item: string) => void
}

const SelectorList: SFC<Props> = props => {
  const {items, selectedItems, onSelectItem} = props

  return (
    <div className="selector-list">
      <FancyScrollbar>
        {items.map(item => {
          const selectedClass = selectedItems.includes(item) ? 'selected' : ''
          const title = selectedItems.includes(item)
            ? 'Click to remove this filter'
            : `Click to filter by ${item}`

          return (
            <div
              className={`selector-list--item ${selectedClass}`}
              key={item}
              onClick={() => onSelectItem(item)}
              title={title}
            >
              {item}
            </div>
          )
        })}
      </FancyScrollbar>
    </div>
  )
}

export default SelectorList
