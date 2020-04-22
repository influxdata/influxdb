// Libraries
import React, {SFC} from 'react'
import classnames from 'classnames'

// Components
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'

interface Props {
  items: string[]
  selectedItems: string[]
  onSelectItem: (item: string) => void
  multiSelect: boolean
  children?: JSX.Element | JSX.Element[]
}

const SelectorList: SFC<Props> = props => {
  const {items, selectedItems, onSelectItem, multiSelect, children} = props

  return (
    <BuilderCard.Body addPadding={false} autoHideScrollbars={true}>
      {items.map(item => {
        const className = classnames('selector-list--item', {
          selected: selectedItems.includes(item),
          'selector-list--checkbox': multiSelect,
        })

        const title = selectedItems.includes(item)
          ? 'Click to remove this filter'
          : `Click to filter by ${item}`

        return (
          <div
            className={className}
            data-testid={`selector-list ${item}`}
            key={item}
            onClick={() => onSelectItem(item)}
            title={title}
          >
            {item}
          </div>
        )
      })}
      {children}
    </BuilderCard.Body>
  )
}

export default SelectorList
