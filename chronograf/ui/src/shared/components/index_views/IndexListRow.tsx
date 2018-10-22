// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  disabled?: boolean
  children: JSX.Element[] | JSX.Element
}

@ErrorHandling
class IndexListRow extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    disabled: false,
  }

  public render() {
    const {children} = this.props

    return <tr className={this.className}>{children}</tr>
  }

  private get className(): string {
    const {disabled} = this.props

    return classnames('index-list--row', {'index-list--row-disabled': disabled})
  }
}

export default IndexListRow
