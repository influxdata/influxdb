// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  disabled: boolean
  children: JSX.Element[] | JSX.Element
  customClass?: string
  testID: string
}

@ErrorHandling
class IndexListRow extends Component<Props> {
  public static defaultProps = {
    disabled: false,
    testID: 'table-row',
  }

  public render() {
    const {children, testID} = this.props

    return (
      <tr data-testid={testID} className={this.className}>
        {children}
      </tr>
    )
  }

  private get className(): string {
    const {disabled, customClass} = this.props

    return classnames('index-list--row', {
      'index-list--row-disabled': disabled,
      [customClass]: !!customClass,
    })
  }
}

export default IndexListRow
