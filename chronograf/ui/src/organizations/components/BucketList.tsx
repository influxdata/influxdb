// Libraries
import React, {PureComponent} from 'react'

// Components
import IndexList from 'src/shared/components/index_views/IndexList'

export interface PrettyBucket {
  name: string
  rp: string
  retentionPeriod: string
}

interface Props {
  buckets: PrettyBucket[]
  emptyState: JSX.Element
}

export default class BucketList extends PureComponent<Props> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Name" width="50%" />
          <IndexList.HeaderCell columnName="Retention Period" width="50%" />
        </IndexList.Header>
        <IndexList.Body columnCount={2} emptyState={this.props.emptyState}>
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    return this.props.buckets.map(bucket => (
      <IndexList.Row key={bucket.name}>
        <IndexList.Cell>{bucket.name}</IndexList.Cell>
        <IndexList.Cell>{bucket.retentionPeriod}</IndexList.Cell>
      </IndexList.Row>
    ))
  }
}
