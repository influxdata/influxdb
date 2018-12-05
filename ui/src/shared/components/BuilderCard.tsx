// Libraries
import React, {PureComponent} from 'react'

// Components
import BuilderCardSearchBar from 'src/shared/components/BuilderCardSearchBar'
import BuilderCardLimitMessage from 'src/shared/components/BuilderCardLimitMessage'
import WaitingText from 'src/shared/components/WaitingText'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Styles
import 'src/shared/components/BuilderCard.scss'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  items: string[]
  selectedItems: string[]
  onSelectItems: (selection: string[]) => void
  onSearch: (searchTerm: string) => Promise<void>
  status?: RemoteDataState
  emptyText?: string
  limitCount?: number
  singleSelect?: boolean
}

class BuilderCard extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    status: RemoteDataState.Done,
    emptyText: 'None Found',
    limitCount: Infinity,
    singleSelect: false,
  }

  public render() {
    return <div className="builder-card">{this.body}</div>
  }

  private get body(): JSX.Element {
    const {status, onSearch, emptyText} = this.props

    if (status === RemoteDataState.Error) {
      return <div className="builder-card--empty">Failed to load</div>
    }

    if (status === RemoteDataState.NotStarted) {
      return <div className="builder-card--empty">{emptyText}</div>
    }

    return (
      <>
        <BuilderCardSearchBar onSearch={onSearch} />
        {this.items}
      </>
    )
  }

  private get items(): JSX.Element {
    const {status, items, limitCount} = this.props

    if (status === RemoteDataState.Loading) {
      return (
        <div className="builder-card--empty">
          <WaitingText text="Loading" />
        </div>
      )
    }

    return (
      <>
        <div className="builder-card--items">
          {items.length ? (
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
          ) : (
            <div className="builder-card--empty">Nothing found</div>
          )}
        </div>
        <BuilderCardLimitMessage
          itemCount={items.length}
          limitCount={limitCount}
        />
      </>
    )
  }

  private itemClass = (item): string => {
    if (this.props.selectedItems.includes(item)) {
      return 'builder-card--item selected'
    }

    return 'builder-card--item'
  }

  private handleToggleItem = (item: string) => (): void => {
    const {singleSelect, selectedItems, onSelectItems} = this.props

    if (singleSelect && selectedItems[0] === item) {
      onSelectItems([])
    } else if (singleSelect) {
      onSelectItems([item])
    } else if (selectedItems.includes(item)) {
      onSelectItems(selectedItems.filter(x => x !== item))
    } else {
      onSelectItems([...selectedItems, item])
    }
  }
}

export default BuilderCard
