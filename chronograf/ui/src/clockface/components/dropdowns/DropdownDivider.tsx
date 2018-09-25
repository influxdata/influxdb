// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {DropdownChild} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children?: DropdownChild
  id: string
  text?: string
}

@ErrorHandling
class DropdownDivider extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    text: '',
  }

  public render() {
    const {text} = this.props

    return (
      <div className={classnames('dropdown--divider', {line: !text})}>
        {text}
      </div>
    )
  }
}

export default DropdownDivider
