// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Dropdown, DropdownMode} from 'src/clockface'

// Types
import {IconFont, ComponentColor, ComponentSize} from '@influxdata/clockface'

enum CreateOption {
  New = 'New Dashboard',
  Import = 'Import Dashboard',
}

interface Props {
  onNewDashboard: () => void
  onToggleOverlay: () => void
}

export default class CreateDashboardDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        mode={DropdownMode.ActionList}
        titleText={'Create Dashboard'}
        icon={IconFont.Plus}
        buttonColor={ComponentColor.Primary}
        buttonSize={ComponentSize.Small}
        widthPixels={200}
        onChange={this.handleSelect}
      >
        {this.optionItems}
      </Dropdown>
    )
  }

  private get optionItems(): JSX.Element[] {
    return [
      <Dropdown.Item
        id={CreateOption.New}
        key={CreateOption.New}
        value={CreateOption.New}
      >
        {CreateOption.New}
      </Dropdown.Item>,
    ]
  }

  private handleSelect = (selection: CreateOption): void => {
    const {onNewDashboard, onToggleOverlay} = this.props
    switch (selection) {
      case CreateOption.New:
        onNewDashboard()
      case CreateOption.Import:
        onToggleOverlay()
    }
  }
}
