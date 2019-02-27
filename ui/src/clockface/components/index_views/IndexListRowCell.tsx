// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {Alignment} from 'src/clockface/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: any
  alignment?: Alignment
  revealOnHover?: boolean
  testID?: string
}

@ErrorHandling
class IndexListRowCell extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    alignment: Alignment.Left,
    revealOnHover: false,
    testID: 'table-cell',
  }

  public render() {
    const {children, testID} = this.props

    return (
      <td className={this.className}>
        <div className="index-list--cell" data-testid={testID}>
          {children}
        </div>
      </td>
    )
  }

  private get className(): string {
    const {alignment, revealOnHover} = this.props

    return classnames('index-list--row-cell', {
      'index-list--show-hover': revealOnHover,
      'index-list--align-left': alignment === Alignment.Left,
      'index-list--align-center': alignment === Alignment.Center,
      'index-list--align-right': alignment === Alignment.Right,
    })
  }
}

export default IndexListRowCell
