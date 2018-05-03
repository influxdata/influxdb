import classnames from 'classnames'
import React, {PureComponent, MouseEvent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  tagKey: string
  tagValues: string[]
}

interface State {
  isOpen: boolean
  filterText: string
}

@ErrorHandling
class TagListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      filterText: '',
      isOpen: false,
    }

    this.handleEscape = this.handleEscape.bind(this)
    this.handleClickKey = this.handleClickKey.bind(this)
    this.handleFilterText = this.handleFilterText.bind(this)
  }

  public handleClickKey(e: MouseEvent<HTMLElement>) {
    e.stopPropagation()
    this.setState({isOpen: !this.state.isOpen})
  }

  public handleFilterText(e) {
    e.stopPropagation()
    this.setState({
      filterText: e.target.value,
    })
  }

  public handleEscape(e) {
    if (e.key !== 'Escape') {
      return
    }

    e.stopPropagation()
    this.setState({
      filterText: '',
    })
  }

  public handleInputClick(e: MouseEvent<HTMLInputElement>) {
    e.stopPropagation()
  }

  public renderTagValues() {
    const {tagValues} = this.props
    if (!tagValues || !tagValues.length) {
      return <div>no tag values</div>
    }

    const filterText = this.state.filterText.toLowerCase()
    const filtered = tagValues.filter(v => v.toLowerCase().includes(filterText))

    return (
      <div className="query-builder--sub-list">
        <div className="query-builder--filter">
          <input
            className="form-control input-sm"
            placeholder={`Filter within ${this.props.tagKey}`}
            type="text"
            value={this.state.filterText}
            onChange={this.handleFilterText}
            onKeyUp={this.handleEscape}
            onClick={this.handleInputClick}
            spellCheck={false}
            autoComplete="false"
          />
          <span className="icon search" />
        </div>
        {filtered.map(v => {
          return (
            <div
              key={v}
              className={'query-builder--list-item'}
              data-test={`query-builder-list-item-tag-value-${v}`}
            >
              {v}
            </div>
          )
        })}
      </div>
    )
  }

  public render() {
    const {tagKey, tagValues} = this.props
    const {isOpen} = this.state
    const tagItemLabel = `${tagKey} — ${tagValues.length}`

    return (
      <div>
        <div
          className={classnames('query-builder--list-item', {active: isOpen})}
          onClick={this.handleClickKey}
          data-test={`query-builder-list-item-tag-${tagKey}`}
        >
          <span>
            <div className="query-builder--caret icon caret-right" />
            {tagItemLabel}
          </span>
        </div>
        {isOpen ? this.renderTagValues() : null}
      </div>
    )
  }
}

export default TagListItem
