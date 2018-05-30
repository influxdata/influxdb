import React, {PureComponent, MouseEvent, ChangeEvent} from 'react'

import {tagKeys as fetchTagKeys} from 'src/shared/apis/v2/metaQueries'
import parseValuesColumn from 'src/shared/parsing/v2/tags'
import TagList from 'src/ifql/components/TagList'
import LoaderSkeleton from 'src/ifql/components/LoaderSkeleton'
import {Service, SchemaFilter, RemoteDataState} from 'src/types'

interface Props {
  db: string
  service: Service
  tagKey: string
  value: string
  filter: SchemaFilter[]
}

interface State {
  isOpen: boolean
  tags: string[]
  loading: RemoteDataState
  searchTerm: string
}

class TagValueListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
      tags: [],
      loading: RemoteDataState.NotStarted,
      searchTerm: '',
    }
  }

  public render() {
    const {db, service, value} = this.props
    const {searchTerm} = this.state

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
              <>
                <div className="ifql-schema--filter">
                  <input
                    className="form-control input-sm"
                    placeholder={`Filter within ${db}`}
                    type="text"
                    spellCheck={false}
                    autoComplete="off"
                    value={searchTerm}
                    onClick={this.handleInputClick}
                    onChange={this.onSearch}
                  />
                </div>
                <TagList
                  db={db}
                  service={service}
                  tags={this.tags}
                  filter={this.filter}
                />
              </>
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
    const {filter, tagKey, value} = this.props

    return [...filter, {key: tagKey, value}]
  }

  private get tags(): string[] {
    const {tags, searchTerm} = this.state
    const term = searchTerm.toLocaleLowerCase()
    return tags.filter(t => t.toLocaleLowerCase().includes(term))
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

  private handleInputClick = (e: MouseEvent<HTMLInputElement>) => {
    e.stopPropagation()
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>) => {
    e.stopPropagation()

    if (this.isFetchable) {
      this.getTags()
    }

    this.setState({isOpen: !this.state.isOpen})
  }

  private onSearch = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({
      searchTerm: e.target.value,
    })
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
