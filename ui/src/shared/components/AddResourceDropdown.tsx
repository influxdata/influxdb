// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {
  IconFont,
  ComponentColor,
  ComponentSize,
  Dropdown,
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
        widthPixels={160}
        button={(active, onClick) => (
          <Dropdown.Button
            status={status}
            active={active}
            onClick={onClick}
            color={ComponentColor.Primary}
            size={ComponentSize.Small}
            icon={IconFont.Plus}
          >
            {titleText || `Create ${this.props.resourceName}`}
          </Dropdown.Button>
        )}
        menu={onCollapse => (
          <Dropdown.Menu onCollapse={onCollapse} overrideWidth={160}>
            {this.dropdownMenuOptions}
          </Dropdown.Menu>
        )}
      />
    )
  }

  private get dropdownMenuOptions(): JSX.Element[] {
    const {
      resourceName,
      onSelectNew,
      onSelectImport,
      onSelectTemplate,
      canImportFromTemplate,
    } = this.props

    const items = [
      <Dropdown.Item
        key="add-resource--new"
        value={null}
        testID="dropdown--item new"
        onClick={onSelectNew}
      >
        {`New ${resourceName}`}
      </Dropdown.Item>,
      <Dropdown.Item
        key="add-resource--import"
        value={null}
        testID="dropdown--item import"
        onClick={onSelectImport}
      >
        {`Import ${resourceName}`}
      </Dropdown.Item>,
    ]

    if (canImportFromTemplate) {
      items.push(
        <Dropdown.Item
          key="add-resource--template"
          value={null}
          testID="dropdown--item template"
          onClick={onSelectTemplate}
        >
          {`From a Template`}
        </Dropdown.Item>
      )
    }

    return items
  }
}
