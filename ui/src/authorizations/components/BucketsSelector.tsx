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
      <BuilderCard
        className="bucket-selectors"
        testID={'builder-card--' + title}
      >
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
                testID="button--select-all"
              />
              <Button
                text="Deselect All"
                size={ComponentSize.ExtraSmall}
                className="bucket-selectors--button"
                onClick={onDeselectAll}
                testID="button-deselect-all"
              />
            </ComponentSpacer>
          </div>
        </BuilderCard.Header>
        <BuilderCard.Menu>
          <Input
            value={searchTerm}
            onChange={this.handleSetSearchTerm}
            placeholder="Search buckets..."
            testID="input-field--bucket-read-filter"
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
