// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {Alignment} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  columnName?: string
  alignment?: Alignment
  width: string
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
      <th className={this.className} style={{width}}>
        {columnName}
      </th>
    )
  }

  private get className(): string {
    const {alignment} = this.props

    return classnames('index-list--header-cell', {
      'index-list--align-left': alignment === Alignment.Left,
      'index-list--align-center': alignment === Alignment.Center,
      'index-list--align-right': alignment === Alignment.Right,
    })
  }
}

export default IndexListHeaderCell
