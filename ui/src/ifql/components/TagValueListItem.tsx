import React, {PureComponent, MouseEvent} from 'react'

import {tagKeys as fetchTagKeys} from 'src/shared/apis/v2/metaQueries'
import parseValuesColumn from 'src/shared/parsing/v2/tags'
import TagList from 'src/ifql/components/TagList'
import LoaderSkeleton from 'src/ifql/components/LoaderSkeleton'
import {Service, SchemaFilter, RemoteDataState} from 'src/types'

interface Props {
  db: string
  service: Service
  tag: string
  value: string
  filter: SchemaFilter[]
}

interface State {
  isOpen: boolean
  tags: string[]
  loading: RemoteDataState
}

class TagValueListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
      tags: [],
      loading: RemoteDataState.NotStarted,
    }
  }

  public render() {
    const {db, service, value} = this.props
    const {tags} = this.state

    return (
      <div className={this.className} onClick={this.handleClick}>
        <div className="ifql-schema-item">
          <div className="ifql-schema-item-toggle" />
          {value}
          <span className="ifql-schema-type">Tag Value</span>
        </div>
        {this.state.isOpen && (
          <>
            {this.isLoading && <LoaderSkeleton />}
            {!this.isLoading && (
              <TagList
                db={db}
                service={service}
                tags={tags}
                filter={this.filter}
              />
            )}
          </>
        )}
      </div>
    )
  }

  private get isLoading(): boolean {
    return this.state.loading === RemoteDataState.Loading
  }

  private get filter(): SchemaFilter[] {
    const {filter, tag, value} = this.props

    return [...filter, {key: tag, value}]
  }

  private async getTags() {
    const {db, service} = this.props

    this.setState({loading: RemoteDataState.Loading})

    try {
      const response = await fetchTagKeys(service, db, this.filter)
      const tags = parseValuesColumn(response)
      this.setState({tags, loading: RemoteDataState.Done})
    } catch (error) {
      console.error(error)
    }
  }

  private get className(): string {
    const {isOpen} = this.state
    const openClass = isOpen ? 'expanded' : ''

    return `ifql-schema-tree ifql-tree-node ${openClass}`
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
}

export default TagValueListItem
