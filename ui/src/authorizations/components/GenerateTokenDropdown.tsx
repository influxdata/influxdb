// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Dropdown, DropdownMode} from 'src/clockface'

// Types
import {IconFont, ComponentColor, ComponentSize} from '@influxdata/clockface'

interface OwnProps {
  onSelectAllAccess: () => void
  onSelectReadWrite: () => void
}

type Props = OwnProps

export default class GenerateTokenDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        mode={DropdownMode.ActionList}
        titleText="Generate"
        icon={IconFont.Plus}
        buttonColor={ComponentColor.Primary}
        buttonSize={ComponentSize.Small}
        widthPixels={160}
        onChange={this.handleSelect}
      >
        {this.optionItems}
      </Dropdown>
    )
  }

  private get optionItems(): JSX.Element[] {
    return [
      <Dropdown.Item
        id={this.bucketReadWriteOption}
        key={this.bucketReadWriteOption}
        value={this.bucketReadWriteOption}
      >
        {this.bucketReadWriteOption}
      </Dropdown.Item>,
      <Dropdown.Item
        id={this.allAccessOption}
        key={this.allAccessOption}
        value={this.allAccessOption}
      >
        {this.allAccessOption}
      </Dropdown.Item>,
    ]
  }

  private get bucketReadWriteOption(): string {
    return 'Read/Write Token'
  }

  private get allAccessOption(): string {
    return 'All Access Token'
  }

  private handleSelect = (selection: string): void => {
    if (selection === this.allAccessOption) {
      this.props.onSelectAllAccess()
    } else if (selection === this.bucketReadWriteOption) {
      this.props.onSelectReadWrite()
    }
  }
}
