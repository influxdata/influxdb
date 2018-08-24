// Libraries
import React, {
  PureComponent,
  CSSProperties,
  ChangeEvent,
  MouseEvent,
} from 'react'
import _ from 'lodash'
import {CopyToClipboard} from 'react-copy-to-clipboard'

// Components
import TagValueList from 'src/flux/components/TagValueList'
import LoaderSkeleton from 'src/flux/components/LoaderSkeleton'
import LoadingSpinner from 'src/flux/components/LoadingSpinner'

// APIs
import {tagValues as fetchTagValues} from 'src/shared/apis/flux/metaQueries'
import parseValuesColumn from 'src/shared/parsing/flux/values'

// Constants
import {
  copyToClipboardSuccess,
  copyToClipboardFailed,
} from 'src/shared/copy/notifications'
import {explorer} from 'src/flux/constants'

// Types
import {NotificationAction} from 'src/types'
import {SchemaFilter, RemoteDataState} from 'src/types'
import {Source} from 'src/types/v2'

interface Props {
  tagKey: string
  db: string
  source: Source
  filter: SchemaFilter[]
  notify: NotificationAction
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
  private debouncedOnSearch: () => void

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

  public render() {
    const {tagKey, db, source, filter, notify} = this.props
    const {
      tagValues,
      searchTerm,
      loadingMore,
      count,
      limit,
      isOpen,
    } = this.state

    return (
      <div className={this.className}>
        <div className="flux-schema--item" onClick={this.handleClick}>
          <div className="flex-schema-item-group">
            <div className="flux-schema--expander" />
            {tagKey}
            <span className="flux-schema--type">Tag Key</span>
          </div>
          <CopyToClipboard text={tagKey} onCopy={this.handleCopyAttempt}>
            <div className="flux-schema-copy" onClick={this.handleClickCopy}>
              <span className="icon duplicate" title="copy to clipboard" />
              Copy
            </div>
          </CopyToClipboard>
        </div>
        <div className={`flux-schema--children ${isOpen ? '' : 'hidden'}`}>
          <div className="flux-schema--header" onClick={this.handleInputClick}>
            <div className="flux-schema--filter">
              <input
                className="form-control input-xs"
                placeholder={`Filter within ${tagKey}`}
                type="text"
                spellCheck={false}
                autoComplete="off"
                value={searchTerm}
                onChange={this.onSearch}
              />
              {this.isSearching && <LoadingSpinner style={this.spinnerStyle} />}
            </div>
            {this.count}
          </div>
          {this.isLoading && <LoaderSkeleton />}
          {!this.isLoading && (
            <TagValueList
              db={db}
              notify={notify}
              source={source}
              values={tagValues}
              tagKey={tagKey}
              filter={filter}
              onLoadMoreValues={this.handleLoadMoreValues}
              isLoadingMoreValues={loadingMore === RemoteDataState.Loading}
              shouldShowMoreValues={limit < count}
              loadMoreCount={this.loadMoreCount}
            />
          )}
        </div>
      </div>
    )
  }

  private get count(): JSX.Element {
    const {count} = this.state

    if (!count) {
      return
    }

    let pluralizer = 's'

    if (count === 1) {
      pluralizer = ''
    }

    return (
      <div className="flux-schema--count">{`${count} Tag Value${pluralizer}`}</div>
    )
  }

  private get spinnerStyle(): CSSProperties {
    return {
      position: 'absolute',
      right: '18px',
      top: '11px',
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

  private handleInputClick = (e: MouseEvent<HTMLDivElement>): void => {
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
    const {db, source, tagKey, filter} = this.props
    const {searchTerm, limit} = this.state
    const response = await fetchTagValues({
      source,
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
      this.getCount()
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

  private handleClickCopy = e => {
    e.stopPropagation()
  }

  private handleCopyAttempt = (
    copiedText: string,
    isSuccessful: boolean
  ): void => {
    const {notify} = this.props
    if (isSuccessful) {
      notify(copyToClipboardSuccess(copiedText))
    } else {
      notify(copyToClipboardFailed(copiedText))
    }
  }

  private async getCount() {
    const {source, db, filter, tagKey} = this.props
    const {limit, searchTerm} = this.state
    try {
      const response = await fetchTagValues({
        source,
        db,
        filter,
        tagKey,
        limit,
        searchTerm,
        count: true,
      })

      const parsed = parseValuesColumn(response)

      if (parsed.length !== 1) {
        // We expect to never reach this state; instead, the Flux server should
        // return a non-200 status code is handled earlier (after fetching).
        // This return guards against some unexpected behavior---the Flux server
        // returning a 200 status code but ALSO having an error in the CSV
        // response body
        return
      }

      const count = Number(parsed[0])

      this.setState({count})
    } catch (error) {
      console.error(error)
    }
  }

  private get loadMoreCount(): number {
    const {count, limit} = this.state

    return Math.min(Math.abs(count - limit), explorer.TAG_VALUES_LIMIT)
  }

  private get isFetchable(): boolean {
    const {isOpen, loadingAll} = this.state

    return (
      !isOpen &&
      (loadingAll === RemoteDataState.NotStarted ||
        loadingAll === RemoteDataState.Error)
    )
  }

  private get className(): string {
    const {isOpen} = this.state
    const openClass = isOpen ? 'expanded' : ''

    return `flux-schema-tree flux-schema--child ${openClass}`
  }
}
