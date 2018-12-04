// Libraries
import React, {PureComponent} from 'react'

// Components
import SelectorCard from 'src/shared/components/SelectorCard'

// Styles
import 'src/shared/components/TimeMachineQueryBuilder.scss'

// Types
import {RemoteDataState} from 'src/types'

const DUMMY_BUCKETS = [
  'Array',
  'Int8Array',
  'Uint8Array',
  'Uint8ClampedArray',
  'Int16Array',
  'Uint16Array',
  'Int32Array',
  'Uint32Array',
  'Float32Array',
  'Float64Array',
]

interface Props {}

interface State {
  buckets: string[]
  bucketsStatus: RemoteDataState
  selectedBuckets: string[]
}

class TimeMachineQueryBuilder extends PureComponent<Props, State> {
  public state: State = {
    buckets: DUMMY_BUCKETS,
    bucketsStatus: RemoteDataState.NotStarted,
    selectedBuckets: [DUMMY_BUCKETS[0], DUMMY_BUCKETS[1]],
  }

  public render() {
    const {buckets, bucketsStatus, selectedBuckets} = this.state

    return (
      <div className="query-builder">
        <div className="query-builder--panel">
          <div className="query-builder--panel-header">Select a Bucket</div>
          <SelectorCard
            status={bucketsStatus}
            items={buckets}
            selectedItems={selectedBuckets}
            onSelectItems={this.handleSelectBuckets}
            onSearch={this.handleSearchBuckets}
            emptyText="No buckets found"
          />
        </div>
        <div className="query-builder--panel">
          <div className="query-builder--panel-header">Select Measurements</div>
          <SelectorCard
            items={[]}
            selectedItems={[]}
            onSelectItems={() => {}}
            onSearch={() => Promise.resolve()}
            emptyText="No measurements found"
          />
        </div>
        <div className="query-builder--panel">
          <div className="query-builder--panel-header">
            Select Measurement Fields
          </div>
          <SelectorCard
            items={[]}
            selectedItems={[]}
            onSelectItems={() => {}}
            onSearch={() => Promise.resolve()}
            emptyText="No fields found"
          />
        </div>
        <div className="query-builder--panel">
          <div className="query-builder--panel-header">Select Functions</div>
          <SelectorCard
            items={[]}
            selectedItems={[]}
            onSelectItems={() => {}}
            onSearch={() => Promise.resolve()}
            emptyText="Select at least one bucket and measurement"
          />
        </div>
      </div>
    )
  }

  private handleSelectBuckets = (selectedBuckets: string[]) => {
    this.setState({selectedBuckets})
  }

  private handleSearchBuckets = async (searchTerm: string) => {
    await new Promise(res => setTimeout(res, 350))

    if (searchTerm === '') {
      this.setState({buckets: DUMMY_BUCKETS})

      return
    }

    this.setState({
      buckets: this.state.buckets.filter(b => {
        return b.toLowerCase().includes(searchTerm.toLowerCase())
      }),
    })
  }
}

export default TimeMachineQueryBuilder
