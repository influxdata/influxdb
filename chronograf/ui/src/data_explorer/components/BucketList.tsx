import React, {Component} from 'react'

import _ from 'lodash'

import {QueryConfig} from 'src/types'
import {Source, Bucket} from 'src/types/v2'
import {Namespace} from 'src/types/queries'

import DatabaseListItem from 'src/shared/components/DatabaseListItem'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {getBuckets} from 'src/shared/apis/v2/buckets'

interface Props {
  query: QueryConfig
  source: Source
  onChooseNamespace: (namespace: Namespace) => void
}

interface State {
  buckets: Bucket[]
}

@ErrorHandling
class BucketList extends Component<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      buckets: [],
    }
  }

  public componentDidMount() {
    this.getBucketList()
  }

  public componentDidUpdate({
    query: prevQuery,
    source: prevSource,
  }: {
    query: QueryConfig
    source: Source
  }) {
    const {source: nextSource, query: nextQuery} = this.props
    const differentSource = !_.isEqual(prevSource, nextSource)

    const newMetaQuery =
      nextQuery.rawText &&
      nextQuery.rawText.match(/^(create|drop)/i) &&
      nextQuery.rawText !== prevQuery.rawText

    if (differentSource || newMetaQuery) {
      this.getBucketList()
    }
  }

  public getBucketList = async () => {
    const {source} = this.props

    try {
      const buckets = await getBuckets(source.links.buckets)
      this.setState({buckets})
    } catch (err) {
      console.error(err)
    }
  }

  public handleChooseBucket(namespace: Namespace) {
    return () => this.props.onChooseNamespace(namespace)
  }

  public isActive(query: QueryConfig, {name}: Bucket) {
    return name === query.database
  }

  public render() {
    return (
      <div className="query-builder--column query-builder--column-db">
        <div className="query-builder--heading">DB.RetentionPolicy</div>
        <div className="query-builder--list">
          <FancyScrollbar>
            {this.state.buckets.map(bucket => (
              <DatabaseListItem
                bucket={bucket}
                key={bucket.name}
                isActive={this.isActive(this.props.query, bucket)}
                onChooseNamespace={this.handleChooseBucket}
              />
            ))}
          </FancyScrollbar>
        </div>
      </div>
    )
  }
}

export default BucketList
