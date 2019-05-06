// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {
  IconFont,
  ComponentColor,
  ComponentSize,
  Dropdown,
  DropdownMode,
  ComponentStatus,
} from '@influxdata/clockface'

interface OwnProps {
  onSelectNew: () => void
  onSelectImport: () => void
  onSelectTemplate?: () => void
  resourceName: string
}

interface DefaultProps {
  canImportFromTemplate: boolean
  status: ComponentStatus
  titleText: string
}

type Props = OwnProps & DefaultProps

export default class AddResourceDropdown extends PureComponent<Props> {
  public static defaultProps: DefaultProps = {
    canImportFromTemplate: false,
    status: ComponentStatus.Default,
    titleText: null,
  }

  public render() {
    const {titleText, status} = this.props
    return (
      <Dropdown
        mode={DropdownMode.ActionList}
        titleText={titleText || `Create ${this.props.resourceName}`}
        icon={IconFont.Plus}
        buttonColor={ComponentColor.Primary}
        buttonSize={ComponentSize.Small}
        widthPixels={160}
        onChange={this.handleSelect}
        status={status}
      >
        {this.optionItems}
      </Dropdown>
    )
  }

  private get optionItems(): JSX.Element[] {
    const importOption = this.importOption
    const newOption = this.newOption
    const templateOption = this.templateOption

    const items = [
      <Dropdown.Item
        id={newOption}
        key={newOption}
        value={newOption}
        testID="dropdown--item new"
      >
        {newOption}
      </Dropdown.Item>,
      <Dropdown.Item
        id={importOption}
        key={importOption}
        value={importOption}
        testID="dropdown--item import"
      >
        {importOption}
      </Dropdown.Item>,
    ]

    if (!!this.props.canImportFromTemplate) {
      items.push(
        <Dropdown.Item
          id={templateOption}
          key={templateOption}
          value={templateOption}
          testID="dropdown--item template"
        >
          {templateOption}
        </Dropdown.Item>
      )
    }

    return items
  }

  private get newOption(): string {
    return `New ${this.props.resourceName}`
  }

  private get importOption(): string {
    return `Import ${this.props.resourceName}`
  }

  private get templateOption(): string {
    return `From a Template`
  }

  private handleSelect = (selection: string): void => {
    const {onSelectNew, onSelectImport, onSelectTemplate} = this.props

    if (selection === this.newOption) {
      onSelectNew()
    }
    if (selection === this.importOption) {
      onSelectImport()
    }
    if (selection == this.templateOption) {
      onSelectTemplate()
    }
  }
}
