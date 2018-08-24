// Libraries
import React, {PureComponent, MouseEvent, ChangeEvent} from 'react'
import {CopyToClipboard} from 'react-copy-to-clipboard'

// Components
import TagList from 'src/flux/components/TagList'
import LoaderSkeleton from 'src/flux/components/LoaderSkeleton'

// APIs
import {tagKeys as fetchTagKeys} from 'src/shared/apis/flux/metaQueries'

// Utils
import parseValuesColumn from 'src/shared/parsing/flux/values'

// Constants
import {
  copyToClipboardSuccess,
  copyToClipboardFailed,
} from 'src/shared/copy/notifications'

// Types
import {Source} from 'src/types/v2'
import {SchemaFilter, RemoteDataState, NotificationAction} from 'src/types'

interface Props {
  db: string
  source: Source
  tagKey: string
  value: string
  filter: SchemaFilter[]
  notify: NotificationAction
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
    const {db, source, value, notify} = this.props
    const {searchTerm, isOpen} = this.state

    return (
      <div className={this.className} onClick={this.handleClick}>
        <div className="flux-schema--item">
          <div className="flex-schema-item-group">
            <div className="flux-schema--expander" />
            {value}
            <span className="flux-schema--type">Tag Value</span>
          </div>
          <CopyToClipboard text={value} onCopy={this.handleCopyAttempt}>
            <div className="flux-schema-copy" onClick={this.handleClickCopy}>
              <span className="icon duplicate" title="copy to clipboard" />
              Copy
            </div>
          </CopyToClipboard>
        </div>
        <div className={`flux-schema--children ${isOpen ? '' : 'hidden'}`}>
          {this.isLoading && <LoaderSkeleton />}
          {!this.isLoading && (
            <>
              {!!this.tags.length && (
                <div className="flux-schema--filter">
                  <input
                    className="form-control input-xs"
                    placeholder={`Filter within ${value}`}
                    type="text"
                    spellCheck={false}
                    autoComplete="off"
                    value={searchTerm}
                    onClick={this.handleInputClick}
                    onChange={this.onSearch}
                  />
                </div>
              )}
              <TagList
                db={db}
                notify={notify}
                source={source}
                tags={this.tags}
                filter={this.filter}
              />
            </>
          )}
        </div>
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
    const {db, source} = this.props

    this.setState({loading: RemoteDataState.Loading})

    try {
      const response = await fetchTagKeys(source, db, this.filter)
      const tags = parseValuesColumn(response)
      this.setState({tags, loading: RemoteDataState.Done})
    } catch (error) {
      console.error(error)
    }
  }

  private get className(): string {
    const {isOpen} = this.state
    const openClass = isOpen ? 'expanded' : ''

    return `flux-schema-tree flux-schema--child ${openClass}`
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
        loading === RemoteDataState.Error)
    )
  }
}

export default TagValueListItem
