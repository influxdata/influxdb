// Libraries
import React, {Component} from 'react'

// Components
import EmptyStateText from 'src/clockface/components/empty_state/EmptyStateText'
import EmptyStateSubText from 'src/clockface/components/empty_state/EmptyStateSubText'

// Types
import {ComponentSize} from 'src/clockface/types'

// Styles
import 'src/clockface/components/empty_state/EmptyState.scss'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface PassedProps {
  children: JSX.Element | JSX.Element[]
}

interface DefaultProps {
  size?: ComponentSize
  testID?: string
}

type Props = PassedProps & DefaultProps

@ErrorHandling
class EmptyState extends Component<Props> {
  public static defaultProps: DefaultProps = {
    size: ComponentSize.Small,
    testID: 'empty-state',
  }

  public static Text = EmptyStateText
  public static SubText = EmptyStateSubText

  public render() {
    const {children, size, testID} = this.props

    const className = `empty-state empty-state--${size}`

    return (
      <div className={className} data-testid={testID}>
        {children}
      </div>
    )
  }
}

export default EmptyState
