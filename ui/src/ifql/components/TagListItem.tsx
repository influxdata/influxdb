import React, {PureComponent, ChangeEvent, MouseEvent} from 'react'

import _ from 'lodash'

import {Service, SchemaFilter, RemoteDataState} from 'src/types'
import {tagValues as fetchTagValues} from 'src/shared/apis/v2/metaQueries'
import parseValuesColumn from 'src/shared/parsing/v2/tags'
import TagValueList from 'src/ifql/components/TagValueList'
import LoaderSkeleton from 'src/ifql/components/LoaderSkeleton'
import SearchSpinner from 'src/ifql/components/SearchSpinner'

interface Props {
  tag: string
  db: string
  service: Service
  filter: SchemaFilter[]
}

interface State {
  isOpen: boolean
  loadingAll: RemoteDataState
  loadingSearch: RemoteDataState
  tagValues: string[]
  searchTerm: string
}

export default class TagListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
      loadingAll: RemoteDataState.NotStarted,
      loadingSearch: RemoteDataState.NotStarted,
      tagValues: [],
      searchTerm: '',
    }

    this.debouncedSearch = _.debounce(this.searchTagValues, 250)
  }

  public render() {
    const {tag, db, service, filter} = this.props
    const {tagValues, searchTerm} = this.state

    return (
      <div className={this.className}>
        <div className="ifql-schema-item" onClick={this.handleClick}>
          <div className="ifql-schema-item-toggle" />
          {tag}
          <span className="ifql-schema-type">Tag Key</span>
        </div>
        {this.state.isOpen && (
          <>
            <div className="ifql-schema--filter">
              <input
                className="form-control input-sm"
                placeholder={`Filter within ${tag}`}
                type="text"
                spellCheck={false}
                autoComplete="off"
                value={searchTerm}
                onClick={this.handleInputClick}
                onChange={this.onSearch}
              />
              {this.isSearching && <SearchSpinner />}
            </div>
            {this.isLoading && <LoaderSkeleton />}
            {!this.isLoading && (
              <TagValueList
                db={db}
                service={service}
                values={tagValues}
                tag={tag}
                filter={filter}
              />
            )}
          </>
        )}
      </div>
    )
  }

  private get isSearching(): boolean {
    return this.state.loadingSearch === RemoteDataState.Loading
  }

  private get isLoading(): boolean {
    return this.state.loadingAll === RemoteDataState.Loading
  }

  private onSearch = (e: ChangeEvent<HTMLInputElement>): void => {
    const searchTerm = e.target.value

    this.setState({searchTerm, loadingSearch: RemoteDataState.Loading}, () =>
      this.debouncedSearch()
    )
  }

  private debouncedSearch() {} // See constructor

  private handleInputClick = (e: MouseEvent<HTMLInputElement>): void => {
    e.stopPropagation()
  }

  private searchTagValues = async () => {
    try {
      const tagValues = await this.getTagValues()

      this.setState({
        tagValues,
        loadingSearch: RemoteDataState.Done,
      })
    } catch (error) {
      console.error(error)
      this.setState({loadingSearch: RemoteDataState.Error})
    }
  }

  private getAllTagValues = async () => {
    this.setState({loadingAll: RemoteDataState.Loading})

    try {
      const tagValues = await this.getTagValues()

      this.setState({
        tagValues,
        loadingAll: RemoteDataState.Done,
      })
    } catch (error) {
      console.error(error)
      this.setState({loadingAll: RemoteDataState.Error})
    }
  }

  private getTagValues = async () => {
    const {db, service, tag, filter} = this.props
    const {searchTerm} = this.state
    const response = await fetchTagValues(service, db, filter, tag, searchTerm)

    return parseValuesColumn(response)
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>) => {
    e.stopPropagation()

    if (this.isFetchable) {
      this.getAllTagValues()
    }

    this.setState({isOpen: !this.state.isOpen})
  }

  private get isFetchable(): boolean {
    const {isOpen, loadingAll} = this.state

    return (
      !isOpen &&
      (loadingAll === RemoteDataState.NotStarted ||
        loadingAll !== RemoteDataState.Error)
    )
  }

  private get className(): string {
    const {isOpen} = this.state
    const openClass = isOpen ? 'expanded' : ''

    return `ifql-schema-tree ifql-tree-node ${openClass}`
  }
}
