// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {DropdownChild} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  id: string
}

interface DefaultProps {
  text?: string
  children?: DropdownChild
}

type Props = DefaultProps & OwnProps

@ErrorHandling
class DropdownDivider extends Component<Props> {
  public static defaultProps: DefaultProps = {
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
