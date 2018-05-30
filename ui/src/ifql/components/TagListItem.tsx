import React, {
  PureComponent,
  CSSProperties,
  ChangeEvent,
  MouseEvent,
} from 'react'

import _ from 'lodash'

import {Service, SchemaFilter, RemoteDataState} from 'src/types'
import {tagValues as fetchTagValues} from 'src/shared/apis/v2/metaQueries'
import {explorer} from 'src/ifql/constants'
import parseValuesColumn from 'src/shared/parsing/v2/tags'
import TagValueList from 'src/ifql/components/TagValueList'
import LoaderSkeleton from 'src/ifql/components/LoaderSkeleton'
import LoadingSpinner from 'src/ifql/components/LoadingSpinner'

interface Props {
  tagKey: string
  db: string
  service: Service
  filter: SchemaFilter[]
}

interface State {
  isOpen: boolean
  loadingAll: RemoteDataState
  loadingSearch: RemoteDataState
  loadingMore: RemoteDataState
  tagValues: string[]
  searchTerm: string
  limit: number
  count: number | null
}

export default class TagListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
      loadingAll: RemoteDataState.NotStarted,
      loadingSearch: RemoteDataState.NotStarted,
      loadingMore: RemoteDataState.NotStarted,
      tagValues: [],
      count: null,
      searchTerm: '',
      limit: explorer.TAG_VALUES_LIMIT,
    }

    this.debouncedOnSearch = _.debounce(() => {
      this.searchTagValues()
      this.getCount()
    }, 250)
  }

  public async componentDidMount() {
    this.getCount()
  }

  public render() {
    const {tagKey, db, service, filter} = this.props
    const {tagValues, searchTerm, loadingMore, count, limit} = this.state

    return (
      <div className={this.className}>
        <div className="ifql-schema-item" onClick={this.handleClick}>
          <div className="ifql-schema-item-toggle" />
          {tagKey}
          <span className="ifql-schema-type">Tag Key</span>
        </div>
        {this.state.isOpen && (
          <>
            <div className="tag-value-list--header">
              <div className="ifql-schema--filter">
                <input
                  className="form-control input-sm"
                  placeholder={`Filter within ${tagKey}`}
                  type="text"
                  spellCheck={false}
                  autoComplete="off"
                  value={searchTerm}
                  onClick={this.handleInputClick}
                  onChange={this.onSearch}
                />
                {this.isSearching && (
                  <LoadingSpinner style={this.spinnerStyle} />
                )}
              </div>
              {!!count && `${count} total`}
            </div>
            {this.isLoading && <LoaderSkeleton />}
            {!this.isLoading && (
              <>
                <TagValueList
                  db={db}
                  service={service}
                  values={tagValues}
                  tagKey={tagKey}
                  filter={filter}
                  onLoadMoreValues={this.handleLoadMoreValues}
                  isLoadingMoreValues={loadingMore === RemoteDataState.Loading}
                  shouldShowMoreValues={limit < count}
                />
              </>
            )}
          </>
        )}
      </div>
    )
  }

  private get spinnerStyle(): CSSProperties {
    return {
      position: 'absolute',
      right: '15px',
      top: '6px',
    }
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
      this.debouncedOnSearch()
    )
  }

  private debouncedOnSearch() {} // See constructor

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

  private getMoreTagValues = async () => {
    this.setState({loadingMore: RemoteDataState.Loading})

    try {
      const tagValues = await this.getTagValues()

      this.setState({
        tagValues,
        loadingMore: RemoteDataState.Done,
      })
    } catch (error) {
      console.error(error)
      this.setState({loadingMore: RemoteDataState.Error})
    }
  }

  private getTagValues = async () => {
    const {db, service, tagKey, filter} = this.props
    const {searchTerm, limit} = this.state
    const response = await fetchTagValues({
      service,
      db,
      filter,
      tagKey,
      limit,
      searchTerm,
    })

    return parseValuesColumn(response)
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>) => {
    e.stopPropagation()

    if (this.isFetchable) {
      this.getAllTagValues()
    }

    this.setState({isOpen: !this.state.isOpen})
  }

  private handleLoadMoreValues = (): void => {
    const {limit} = this.state

    this.setState(
      {limit: limit + explorer.TAG_VALUES_LIMIT},
      this.getMoreTagValues
    )
  }

  private async getCount() {
    const {service, db, filter, tagKey} = this.props
    const {limit, searchTerm} = this.state
    try {
      const response = await fetchTagValues({
        service,
        db,
        filter,
        tagKey,
        limit,
        searchTerm,
        count: true,
      })

      const parsed = parseValuesColumn(response)

      if (parsed.length !== 1) {
        throw new Error('Unexpected count response')
      }

      const count = Number(parsed[0])

      this.setState({count})
    } catch (error) {
      console.error(error)
    }
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
