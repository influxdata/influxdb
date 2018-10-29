// Libraries
import React, {Component} from 'react'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children?: JSX.Element[] | JSX.Element
  emptyState: JSX.Element
  columnCount: number
}

@ErrorHandling
class IndexListBody extends Component<Props> {
  public render() {
    const {children, columnCount, emptyState} = this.props

    if (React.Children.count(children)) {
      return <tbody className="index-list--body">{children}</tbody>
    }

    return (
      <tbody className="index-list--empty">
        <tr className="index-list--empty-row">
          <td colSpan={columnCount}>
            <div className="index-list--empty-cell" data-test="empty-state">
              {emptyState}
            </div>
          </td>
        </tr>
      </tbody>
    )
  }
}

export default IndexListBody
