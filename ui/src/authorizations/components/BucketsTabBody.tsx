// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Libraries
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import SelectorList from 'src/timeMachine/components/SelectorList'

// Types
import {Bucket} from 'src/types'
import {Input} from '@influxdata/clockface'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'
import FilterList from 'src/shared/components/FilterList'

interface Props {
  buckets: Bucket[]
  onSelect: (id: string) => void
  selectedBuckets: string[]
}

interface State {
  searchTerm: string
}

const FilterBuckets = FilterList<Bucket>()

class BucketsTabBody extends PureComponent<Props> {
  public state: State = {searchTerm: ''}

  public render() {
    const {selectedBuckets, onSelect, buckets} = this.props
    const {searchTerm} = this.state

    return (
      <>
        <BuilderCard.Menu>
          <Input
            value={searchTerm}
            onChange={this.handleSetSearchTerm}
            placeholder="Search buckets..."
          />
        </BuilderCard.Menu>
        <FilterBuckets
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
        </FilterBuckets>
      </>
    )
  }

  private handleSetSearchTerm = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({searchTerm: e.target.value})
  }
}

export default BucketsTabBody
