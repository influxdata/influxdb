// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Dropdown} from 'src/clockface'

// Types
import {Organization} from 'src/api'

interface Props {
  orgs: Organization[]
  onChangeOrgID: (selectedOrgID: string) => void
  selectedOrgID: string
}

export default class TaskOptionsOrgIDDropdown extends PureComponent<Props> {
  public componentDidMount() {
    this.setSelectedToFirst()
  }

  public componentDidUpdate(prevProps: Props) {
    if (this.props.orgs !== prevProps.orgs) {
      this.setSelectedToFirst()
    }
  }
  public render() {
    const {selectedOrgID, onChangeOrgID} = this.props
    return (
      <Dropdown selectedID={selectedOrgID} onChange={onChangeOrgID}>
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

  private setSelectedToFirst() {
    const {orgs, onChangeOrgID} = this.props
    const firstOrgIDInList = _.get(orgs, '0.id', '')

    onChangeOrgID(firstOrgIDInList)
  }
}
