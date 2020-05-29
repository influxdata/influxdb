// Libraries
import React, {PureComponent} from 'react'

interface Props {
  title: string
  testID: string
  onDelete?: () => void
  onDragStart?: () => void
}

export default class SelectorListHeader extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'selector-list--header',
  }

  public render() {
    const {testID, children} = this.props

    return (
      <div className="selector-list--header" data-testid={testID}>
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
        <div className="selector-list--draggable" onDragStart={onDragStart}>
          <div className="selector-list--hamburger" />
          <h2 className="selector-list--title">{title}</h2>
        </div>
      )
    }

    return <h2 className="selector-list--title">{title}</h2>
  }

  private get deleteButton(): JSX.Element | undefined {
    const {onDelete} = this.props

    if (onDelete) {
      return <div className="selector-list--delete" onClick={onDelete} />
    }
  }
}
