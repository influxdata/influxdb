// Libraries
import React, {PureComponent} from 'react'

interface Props {
  title: string
  testID: string
  onDelete?: () => void
  onDragStart?: () => void
}

export default class BuilderCardHeader extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'builder-card--header',
  }

  public render() {
    const {testID, children} = this.props

    return (
      <div className="builder-card--header" data-testid={testID}>
        {this.title}
        {children}
        {this.deleteButton}
      </div>
    )
  }

  private get title(): JSX.Element {
    const {onDragStart, title} = this.props

    if (onDragStart) {
      return (
        <div className="builder-card--draggable" onDragStart={onDragStart}>
          <div className="builder-card--hamburger" />
          <h2 className="builder-card--title">{title}</h2>
        </div>
      )
    }

    return <h2 className="builder-card--title">{title}</h2>
  }

  private get deleteButton(): JSX.Element | undefined {
    const {onDelete} = this.props

    if (onDelete) {
      return <div className="builder-card--delete" onClick={onDelete} />
    }
  }
}
