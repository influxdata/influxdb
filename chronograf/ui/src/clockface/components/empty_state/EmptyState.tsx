// Libraries
import React, {Component} from 'react'

// Components
import EmptyStateText from 'src/clockface/components/empty_state/EmptyStateText'
import EmptyStateSubText from 'src/clockface/components/empty_state/EmptyStateSubText'

// Types
import {ComponentSize} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  size?: ComponentSize
  children: JSX.Element | JSX.Element[]
}

@ErrorHandling
class EmptyState extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    size: ComponentSize.Small,
  }

  public static Text = EmptyStateText
  public static SubText = EmptyStateSubText

  public render() {
    const {children, size} = this.props

    const className = `empty-state empty-state--${size}`

    return <div className={className}>{children}</div>
  }
}

export default EmptyState
