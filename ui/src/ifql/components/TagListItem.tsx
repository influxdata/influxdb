import React, {PureComponent, CSSProperties, MouseEvent} from 'react'

import {Service, SchemaFilter} from 'src/types'
import {tagsFromMeasurement} from 'src/shared/apis/v2/metaQueries'
import parseTags from 'src/shared/parsing/v2/tags'
import TagList from 'src/ifql/components/TagList'

interface Props {
  tag: string
  db: string
  service: Service
  filter: SchemaFilter[]
}

interface State {
  isOpen: boolean
  loading: string
  tags: string[]
}

enum RemoteDataState {
  NotStarted = 'NotStarted',
  Loading = 'Loading',
  Done = 'Done',
  Error = 'Error',
}

export default class TagListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
      loading: RemoteDataState.NotStarted,
      tags: [],
    }
  }

  public render() {
    const {tag, db, service} = this.props
    const {tags} = this.state

    return (
      <div className={this.className} style={this.style}>
        <div className="ifql-schema-item" onClick={this.handleClick}>
          <div className="ifql-schema-item-toggle" />
          {tag}
          <TagList db={db} service={service} tags={tags} />
        </div>
      </div>
    )
  }

  private async getTags() {
    const {db, service, tag, filter} = this.props

    try {
      const response = await tagsFromMeasurement(service, db, measurement)
      const tags = parseTags(response)
      this.setState({
        tags,
        loading: RemoteDataState.Done,
      })
    } catch (error) {
      console.error(error)
    }
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>) => {
    e.stopPropagation()

    if (this.isFetchable) {
      this.getTags()
    }

    this.setState({isOpen: !this.state.isOpen})
  }

  private get isFetchable(): boolean {
    const {isOpen, loading} = this.state

    return (
      !isOpen &&
      (loading === RemoteDataState.NotStarted ||
        loading !== RemoteDataState.Error)
    )
  }

  private get className(): string {
    const {isOpen} = this.state
    const openClass = isOpen ? 'expanded' : ''

    return `ifql-schema-tree ifql-tree-node ${openClass}`
  }
}
