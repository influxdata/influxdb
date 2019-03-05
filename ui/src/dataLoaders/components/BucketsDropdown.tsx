// Libraries
import React, {PureComponent} from 'react'

// Components
import {Dropdown, ComponentStatus} from 'src/clockface'

// Types
import {Bucket} from '@influxdata/influx'

interface Props {
  selectedBucketID: string
  buckets: Bucket[]
  onSelectBucket: (bucket: Bucket) => void
}

class BucketsDropdown extends PureComponent<Props> {
  public render() {
    const {selectedBucketID, onSelectBucket} = this.props

    return (
      <Dropdown
        titleText={this.title}
        status={this.status}
        selectedID={selectedBucketID}
        onChange={onSelectBucket}
      >
        {this.dropdownBuckets}
      </Dropdown>
    )
  }

  private get title(): string {
    if (this.isBucketsEmpty) {
      return 'No buckets found'
    }

    return ''
  }

  private get status(): ComponentStatus {
    if (this.isBucketsEmpty) {
      return ComponentStatus.Disabled
    }

    return ComponentStatus.Default
  }

  private get isBucketsEmpty(): boolean {
    const {buckets} = this.props
    return !buckets || !buckets.length
  }

  private get dropdownBuckets(): JSX.Element[] {
    const {buckets} = this.props
    if (this.isBucketsEmpty) {
      return []
    }

    return buckets.map(b => (
      <Dropdown.Item key={b.name} value={b} id={b.id}>
        {b.name}
      </Dropdown.Item>
    ))
  }
}

export default BucketsDropdown
