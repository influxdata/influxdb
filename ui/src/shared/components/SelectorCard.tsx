// Libraries
import React, {PureComponent} from 'react'

// Components
import SelectorCardSearchBar from 'src/shared/components/SelectorCardSearchBar'
import WaitingText from 'src/shared/components/WaitingText'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Styles
import 'src/shared/components/SelectorCard.scss'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  items: string[]
  selectedItems: string[]
  onSelectItems: (selection: string[]) => void
  status?: RemoteDataState
  onSearch?: (searchTerm: string) => Promise<void>
  emptyText?: string
  className?: string
}

class SelectorCard extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    className: '',
    status: RemoteDataState.Done,
    emptyText: 'None Found',
  }

  public render() {
    const {status, items, onSearch, emptyText} = this.props

    if (status === RemoteDataState.Loading) {
      return (
        <div className={this.containerClass}>
          <div className="selector-card--empty selector-card--loading">
            <WaitingText text="Loading" />
          </div>
        </div>
      )
    }

    if (status === RemoteDataState.Done && !items.length) {
      return (
        <div className={this.containerClass}>
          <div className="selector-card--empty">{emptyText}</div>
        </div>
      )
    }

    return (
      <div className={this.containerClass}>
        <div className="selector-card--header">
          {onSearch && <SelectorCardSearchBar onSearch={onSearch} />}
        </div>
        <div className="selector-card--items">
          <FancyScrollbar>
            {items.map(item => (
              <div
                key={item}
                className={this.itemClass(item)}
                onClick={this.handleToggleItem(item)}
              >
                {item}
              </div>
            ))}
          </FancyScrollbar>
        </div>
      </div>
    )
  }

  private get containerClass() {
    const {className} = this.props

    return `selector-card ${className}`
  }

  private itemClass = (item): string => {
    if (this.props.selectedItems.includes(item)) {
      return 'selector-card--item selected'
    }

    return 'selector-card--item'
  }

  private handleToggleItem = (item: string) => (): void => {
    const {selectedItems, onSelectItems} = this.props

    if (selectedItems.includes(item)) {
      onSelectItems(selectedItems.filter(x => x !== item))
    } else {
      onSelectItems([...selectedItems, item])
    }
  }
}

export default SelectorCard
