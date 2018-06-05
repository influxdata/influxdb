import React, {PureComponent, ChangeEvent, MouseEvent} from 'react'
import classnames from 'classnames'

import {CopyToClipboard} from 'react-copy-to-clipboard'
import {tagKeys as fetchTagKeys} from 'src/shared/apis/flux/metaQueries'
import parseValuesColumn from 'src/shared/parsing/flux/values'
import TagList from 'src/flux/components/TagList'
import {Service} from 'src/types'

interface Props {
  db: string
  service: Service
}

interface State {
  isOpen: boolean
  tags: string[]
  searchTerm: string
  isCopied: boolean
  copiedText: string
}

class DatabaseListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
      tags: [],
      searchTerm: '',
      isCopied: false,
      copiedText: '',
    }
  }

  public async componentDidMount() {
    const {db, service} = this.props

    try {
      const response = await fetchTagKeys(service, db, [])
      const tags = parseValuesColumn(response)
      this.setState({tags})
    } catch (error) {
      console.error(error)
    }
  }

  public render() {
    const {db, service} = this.props
    const {searchTerm} = this.state

    return (
      <div className={this.className} onClick={this.handleClick}>
        <div className="flux-schema-item">
          <div className="flux-schema-item-toggle" />
          {db}
          <span className="flux-schema-type">Bucket</span>
          <CopyToClipboard text={db} onCopy={this.handleCopy}>
            <span
              className="icon duplicate"
              title="copy to clipboard"
              style={{zIndex: 100}}
            />
          </CopyToClipboard>
        </div>
        {this.state.isOpen && (
          <>
            <div className="flux-schema--filter">
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
            <TagList db={db} service={service} tags={this.tags} filter={[]} />
          </>
        )}
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

  private handleCopy = (copiedText: string): void => {
    this.setState({isCopied: true, copiedText})
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
