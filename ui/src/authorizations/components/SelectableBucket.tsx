// Libraries
import React, {PureComponent} from 'react'

// Types
import {Bucket} from '@influxdata/influx'

interface Props {
  bucket: Bucket
  onSelect: (id: string) => void
}

class SelectableBucket extends PureComponent<Props> {
  render() {
    const {bucket} = this.props

    return <div onClick={this.handleClick}>{bucket.name}</div>
  }

  private handleClick = () => {
    const {bucket, onSelect} = this.props
    onSelect(bucket.id)
  }
}

export default SelectableBucket
