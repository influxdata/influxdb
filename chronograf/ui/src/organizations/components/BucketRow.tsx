// Libraries
import React, {PureComponent} from 'react'

// Components
import IndexList from 'src/shared/components/index_views/IndexList'

// Types
import {Bucket, OverlayState} from 'src/types/v2'

export interface PrettyBucket extends Bucket {
  ruleString: string
}

interface Props {
  bucket: PrettyBucket
  onEditBucket: (b: PrettyBucket) => void
}

interface State {
  overlayState: OverlayState
}

export default class BucketRow extends PureComponent<Props, State> {
  public render() {
    const {bucket} = this.props
    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>
            <a href="#" onClick={this.handleEditBucket}>
              <span>{bucket.name}</span>
            </a>
          </IndexList.Cell>
          <IndexList.Cell>{bucket.ruleString}</IndexList.Cell>
        </IndexList.Row>
      </>
    )
  }

  private handleEditBucket = (): void => {
    this.props.onEditBucket(this.props.bucket)
  }
}
