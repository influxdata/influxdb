// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {Alignment} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: any
  alignment?: Alignment
  revealOnHover?: boolean
}

@ErrorHandling
class IndexListRowCell extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    alignment: Alignment.Left,
    revealOnHover: false,
  }

  public render() {
    const {children} = this.props

    return (
      <td className={this.className}>
        <div className="index-list--cell">{children}</div>
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
