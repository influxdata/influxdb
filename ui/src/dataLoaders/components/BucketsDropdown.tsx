// Libraries
import React, {PureComponent} from 'react'

// Components
import {Dropdown, ComponentStatus} from 'src/clockface'

// Types
import {Bucket} from 'src/api'

interface Props {
  selected: string
  buckets: Bucket[]
  onSelectBucket: (bucket: Bucket) => void
}

class BucketsDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        titleText={this.title}
        status={this.status}
        selectedID={this.selectedID}
        onChange={this.handleSelectBucket}
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

  private get selectedID(): string {
    return this.props.selected || ''
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
      <Dropdown.Item key={b.name} value={b.name} id={b.name}>
        {b.name}
      </Dropdown.Item>
    ))
  }

  private handleSelectBucket = (bucketName: string) => {
    const bucket = this.props.buckets.find(b => b.name === bucketName)

    this.props.onSelectBucket(bucket)
  }
}

export default BucketsDropdown
