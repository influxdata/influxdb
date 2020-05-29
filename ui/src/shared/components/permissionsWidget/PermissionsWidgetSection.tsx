// Libraries
import React, {Component} from 'react'

// Components
import {
  Button,
  ComponentSize,
  FlexBox,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'
import PermissionsWidgetItem from 'src/shared/components/permissionsWidget/PermissionsWidgetItem'

// Types
import {PermissionsWidgetMode} from 'src/shared/components/permissionsWidget/PermissionsWidget'

interface Props {
  children: JSX.Element[] | JSX.Element
  mode?: PermissionsWidgetMode
  id: string
  title: string
  onSelectAll?: (sectionID: string) => void
  onDeselectAll?: (sectionID: string) => void
  testID?: string
}

class PermissionsWidgetSection extends Component<Props> {
  public render() {
    const {title, testID} = this.props

    return (
      <section
        className="permissions-widget--section"
        data-testid={testID || 'permissions-section'}
      >
        <header className="permissions-widget--section-heading">
          <h3 className="permissions-widget--section-title">{title}</h3>
          {this.selectionButtons}
        </header>
        <ul className="permissions-widget--section-list">
          {this.sectionItems}
        </ul>
      </section>
    )
  }

  private get sectionItems(): JSX.Element[] {
    const {children, mode} = this.props

    return React.Children.map(children, (child: JSX.Element) => {
      if (child.type !== PermissionsWidgetItem) {
        return null
      }

      return <PermissionsWidgetItem {...child.props} mode={mode} />
    })
  }

  private get selectionButtons(): JSX.Element {
    const {mode, title} = this.props

    if (mode === PermissionsWidgetMode.Write) {
      return (
        <FlexBox
          margin={ComponentSize.Small}
          direction={FlexDirection.Row}
          justifyContent={JustifyContent.FlexEnd}
        >
          <Button
            text="Select All"
            size={ComponentSize.ExtraSmall}
            titleText={`Select all permissions within ${title}`}
            onClick={this.handleSelectAll}
          />
          <Button
            text="Deselect All"
            size={ComponentSize.ExtraSmall}
            titleText={`Deselect all permissions within ${title}`}
            onClick={this.handleDeselectAll}
          />
        </FlexBox>
      )
    }
  }

  private handleSelectAll = (): void => {
    const {id, onSelectAll} = this.props

    if (onSelectAll) {
      onSelectAll(id)
    }
  }

  private handleDeselectAll = (): void => {
    const {id, onDeselectAll} = this.props

    if (onDeselectAll) {
      onDeselectAll(id)
    }
  }
}

export default PermissionsWidgetSection
