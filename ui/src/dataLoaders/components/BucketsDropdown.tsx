// Libraries
import React, {PureComponent} from 'react'

// Components
import {Dropdown, ComponentStatus} from '@influxdata/clockface'

// Utils
import {isSystemBucket} from 'src/buckets/constants/index'

// Types
import {Bucket} from 'src/types'

interface Props {
  selectedBucketID: string
  buckets: Bucket[]
  onSelectBucket: (bucket: Bucket) => void
}

class BucketsDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        testID="bucket-dropdown"
        button={(active, onClick) => (
          <Dropdown.Button
            testID="bucket-dropdown--button"
            active={active}
            onClick={onClick}
            status={this.status}
          >
            {this.selectedBucketName}
          </Dropdown.Button>
        )}
        menu={onCollapse => (
          <Dropdown.Menu onCollapse={onCollapse}>
            {this.dropdownBuckets}
          </Dropdown.Menu>
        )}
      />
    )
  }

  private get selectedBucketName(): string {
    const {selectedBucketID, buckets} = this.props

    if (this.isBucketsEmpty) {
      return 'No buckets found'
    }

    return buckets.find(bucket => bucket.id === selectedBucketID).name
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
    const {buckets, onSelectBucket, selectedBucketID} = this.props

    if (this.isBucketsEmpty) {
      return []
    }

    const nonSystemBuckets = buckets.filter(
      bucket => !isSystemBucket(bucket.name)
    )

    return nonSystemBuckets.map(b => (
      <Dropdown.Item
        key={b.name}
        value={b}
        id={b.id}
        onClick={onSelectBucket}
        selected={b.id === selectedBucketID}
      >
        {b.name}
      </Dropdown.Item>
    ))
  }
}

export default BucketsDropdown
