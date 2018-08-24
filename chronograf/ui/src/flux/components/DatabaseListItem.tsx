import React, {PureComponent, ChangeEvent, MouseEvent} from 'react'
import classnames from 'classnames'
import {CopyToClipboard} from 'react-copy-to-clipboard'

import {tagKeys as fetchTagKeys} from 'src/shared/apis/flux/metaQueries'
import parseValuesColumn from 'src/shared/parsing/flux/values'
import TagList from 'src/flux/components/TagList'
import {
  copyToClipboardSuccess,
  copyToClipboardFailed,
} from 'src/shared/copy/notifications'

import {NotificationAction} from 'src/types'
import {Source} from 'src/types/v2'

interface Props {
  db: string
  source: Source
  notify: NotificationAction
}

interface State {
  isOpen: boolean
  tags: string[]
  searchTerm: string
}

class DatabaseListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
      tags: [],
      searchTerm: '',
    }
  }

  public async componentDidMount() {
    const {db, source} = this.props

    try {
      const response = await fetchTagKeys(source, db, [])
      const tags = parseValuesColumn(response)
      this.setState({tags})
    } catch (error) {
      console.error(error)
    }
  }

  public render() {
    const {db} = this.props

    return (
      <div className={this.className} onClick={this.handleClick}>
        <div className="flux-schema--item">
          <div className="flex-schema-item-group">
            <div className="flux-schema--expander" />
            {db}
            <span className="flux-schema--type">Bucket</span>
          </div>
          <CopyToClipboard text={db} onCopy={this.handleCopyAttempt}>
            <div className="flux-schema-copy" onClick={this.handleClickCopy}>
              <span className="icon duplicate" title="copy to clipboard" />
              Copy
            </div>
          </CopyToClipboard>
        </div>
        {this.filterAndTagList}
      </div>
    )
  }

  private get tags(): string[] {
    const {tags, searchTerm} = this.state
    const term = searchTerm.toLocaleLowerCase()
    return tags.filter(t => t.toLocaleLowerCase().includes(term))
  }

  private get className(): string {
    return classnames('flux-schema-tree', {
      expanded: this.state.isOpen,
    })
  }

  private get filterAndTagList(): JSX.Element {
    const {db, source, notify} = this.props
    const {isOpen, searchTerm} = this.state

    return (
      <div className={`flux-schema--children ${isOpen ? '' : 'hidden'}`}>
        <div className="flux-schema--filter">
          <input
            className="form-control input-xs"
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
          source={source}
          tags={this.tags}
          filter={[]}
          notify={notify}
        />
      </div>
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

  private onSearch = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({
      searchTerm: e.target.value,
    })
  }

  private handleClick = () => {
    this.setState({isOpen: !this.state.isOpen})
  }

  private handleInputClick = (e: MouseEvent<HTMLInputElement>) => {
    e.stopPropagation()
  }
}

export default DatabaseListItem
