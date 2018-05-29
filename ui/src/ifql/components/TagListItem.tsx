import React, {PureComponent, MouseEvent} from 'react'

import {Service, SchemaFilter, RemoteDataState} from 'src/types'
import {tagValues as fetchTagValues} from 'src/shared/apis/v2/metaQueries'
import parseValuesColumn from 'src/shared/parsing/v2/tags'
import TagValueList from 'src/ifql/components/TagValueList'

interface Props {
  tag: string
  db: string
  service: Service
  filter: SchemaFilter[]
}

interface State {
  isOpen: boolean
  loading: string
  tagValues: string[]
}

export default class TagListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
      loading: RemoteDataState.NotStarted,
      tagValues: [],
    }
  }

  public render() {
    const {tag, db, service, filter} = this.props
    const {tagValues} = this.state

    return (
      <div className={this.className}>
        <div className="ifql-schema-item" onClick={this.handleClick}>
          <div className="ifql-schema-item-toggle" />
          {tag}
          <span className="ifql-schema-type">Tag Key</span>
        </div>
        {this.state.isOpen && (
          <TagValueList
            db={db}
            service={service}
            values={tagValues}
            tag={tag}
            filter={filter}
          />
        )}
      </div>
    )
  }

  private async getTagValues() {
    const {db, service, tag, filter} = this.props

    this.setState({loading: RemoteDataState.Loading})

    try {
      const response = await fetchTagValues(service, db, filter, tag)
      const tagValues = parseValuesColumn(response)
      this.setState({
        tagValues,
        loading: RemoteDataState.Done,
      })
    } catch (error) {
      console.error(error)
    }
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>) => {
    e.stopPropagation()

    if (this.isFetchable) {
      this.getTagValues()
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
