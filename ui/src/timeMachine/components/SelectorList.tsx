// Libraries
import React, {SFC} from 'react'

// Components
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'

interface Props {
  items: string[]
  selectedItems: string[]
  onSelectItem: (item: string) => void
}

const SelectorList: SFC<Props> = props => {
  const {items, selectedItems, onSelectItem} = props

  return (
    <BuilderCard.Body addPadding={false}>
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
    </BuilderCard.Body>
  )
}

export default SelectorList
