// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Dropdown} from 'src/clockface'

// Types
import {Organization} from '@influxdata/influx'

interface Props {
  orgs: Organization[]
  onChangeOrgName: (selectedOrg: string) => void
  selectedOrgName: string
}

export default class TaskOptionsOrgDropdown extends PureComponent<Props> {
  public componentDidMount() {
    this.setSelectedToFirst()
  }

  public componentDidUpdate(prevProps: Props) {
    if (this.props.orgs !== prevProps.orgs) {
      this.setSelectedToFirst()
    }
  }
  public render() {
    const {selectedOrgName, onChangeOrgName} = this.props
    return (
      <Dropdown selectedID={selectedOrgName} onChange={onChangeOrgName}>
        {this.orgDropdownItems}
      </Dropdown>
    )
  }

  private get orgDropdownItems(): JSX.Element[] {
    const {orgs} = this.props

    return orgs.map(org => {
      return (
        <Dropdown.Item id={org.name} key={org.name} value={org.name}>
          {org.name}
        </Dropdown.Item>
      )
    })
  }

  private setSelectedToFirst() {
    const {orgs, onChangeOrgName} = this.props
    const firstOrgNameInList = _.get(orgs, '0.name', '')

    onChangeOrgName(firstOrgNameInList)
  }
}
