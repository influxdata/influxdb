// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {Alignment, Sort} from 'src/clockface/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  width: string
  columnName?: string
  alignment?: Alignment
  sort?: Sort
  onClick?: (nextSort: Sort) => void
}

@ErrorHandling
class IndexListHeaderCell extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    columnName: '',
    alignment: Alignment.Left,
  }

  public render() {
    const {columnName, width} = this.props

    return (
      <th className={this.className} style={{width}} onClick={this.handleClick}>
        {columnName}
        {this.sortIndicator}
      </th>
    )
  }

  private handleClick = (): void => {
    const {onClick, sort} = this.props

    if (!onClick || !sort) {
      return
    }

    if (sort === Sort.None) {
      onClick(Sort.Ascending)
    } else if (sort === Sort.Ascending) {
      onClick(Sort.Descending)
    } else if (sort === Sort.Descending) {
      onClick(Sort.None)
    }
  }

  private get sortIndicator(): JSX.Element {
    if (this.isSortable) {
      return (
        <span className="index-list--sort-arrow">
          <span className="icon caret-down" />
        </span>
      )
    }
  }

  private get isSortable(): boolean {
    const {sort} = this.props

    if (
      sort === Sort.None ||
      sort === Sort.Ascending ||
      sort === Sort.Descending
    ) {
      return true
    }

    return false
  }

  private get className(): string {
    const {alignment, sort} = this.props

    return classnames('index-list--header-cell', {
      'index-list--align-left': alignment === Alignment.Left,
      'index-list--align-center': alignment === Alignment.Center,
      'index-list--align-right': alignment === Alignment.Right,
      'index-list--sortable': this.isSortable,
      'index-list--sort-descending': sort === Sort.Descending,
      'index-list--sort-ascending': sort === Sort.Ascending,
    })
  }
}

export default IndexListHeaderCell
