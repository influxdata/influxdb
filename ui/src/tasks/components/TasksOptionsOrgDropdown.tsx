// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Dropdown} from 'src/clockface'

// Types
import {Organization} from 'src/api'

interface Props {
  orgs: Organization[]
  onChangeTaskOrgID: (selectedOrgID: string) => void
  selectedOrgID: string
}

export default class TaskOptionsOrgDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        selectedID={this.selectedID}
        onChange={this.props.onChangeTaskOrgID}
      >
        {this.orgDropdownItems}
      </Dropdown>
    )
  }
  private get orgDropdownItems(): JSX.Element[] {
    const {orgs} = this.props

    return orgs.map(org => {
      return (
        <Dropdown.Item id={org.id} key={org.id} value={org.id}>
          {org.name}
        </Dropdown.Item>
      )
    })
  }

  private get selectedID(): string {
    const {selectedOrgID, orgs} = this.props

    if (!selectedOrgID) {
      return _.get(orgs, '0.id', '')
    }
    return selectedOrgID
  }
}
