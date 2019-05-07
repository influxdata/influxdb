// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Libraries
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import SelectorList from 'src/timeMachine/components/SelectorList'

// Types
import {Bucket} from '@influxdata/influx'
import {
  Input,
  Button,
  ComponentSize,
  ComponentSpacer,
  AlignItems,
  FlexDirection,
} from '@influxdata/clockface'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'
import FilterList from 'src/shared/components/Filter'

interface Props {
  buckets: Bucket[]
  onSelect: (id: string) => void
  onSelectAll: () => void
  onDeselectAll: () => void
  selectedBuckets: string[]
  title: string
}

interface State {
  searchTerm: string
}

class BucketsSelector extends PureComponent<Props> {
  public state: State = {searchTerm: ''}

  render() {
    const {
      selectedBuckets,
      onSelect,
      onSelectAll,
      onDeselectAll,
      title,
      buckets,
    } = this.props
    const {searchTerm} = this.state

    return (
      <BuilderCard className="bucket-selectors">
        <BuilderCard.Header title={title}>
          <div className="bucket-selectors--buttons">
            <ComponentSpacer
              alignItems={AlignItems.Center}
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
            >
              <Button
                text="Select All"
                size={ComponentSize.ExtraSmall}
                className="bucket-selectors--button"
                onClick={onSelectAll}
              />
              <Button
                text="Deselect All"
                size={ComponentSize.ExtraSmall}
                className="bucket-selectors--button"
                onClick={onDeselectAll}
              />
            </ComponentSpacer>
          </div>
        </BuilderCard.Header>
        <BuilderCard.Menu>
          <Input
            value={searchTerm}
            onChange={this.handleSetSearchTerm}
            placeholder="Search buckets..."
          />
        </BuilderCard.Menu>

        <FilterList
          list={buckets}
          searchTerm={searchTerm}
          searchKeys={['name']}
        >
          {filteredBuckets => (
            <SortingHat list={filteredBuckets} sortKey="name">
              {sortedBuckets => (
                <SelectorList
                  items={sortedBuckets.map(b => b.name)}
                  selectedItems={selectedBuckets}
                  onSelectItem={onSelect}
                  multiSelect={false}
                />
              )}
            </SortingHat>
          )}
        </FilterList>
      </BuilderCard>
    )
  }

  private handleSetSearchTerm = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({searchTerm: e.target.value})
  }
}

export default BucketsSelector
