// Libraries
import React, {Component} from 'react'

// Components
import {Button, ComponentSpacer, Alignment, ComponentSize} from 'src/clockface'
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
}

class PermissionsWidgetSection extends Component<Props> {
  public render() {
    const {title} = this.props

    return (
      <section className="permissions-widget--section">
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
        <ComponentSpacer align={Alignment.Left}>
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
        </ComponentSpacer>
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
