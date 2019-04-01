// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Dropdown} from 'src/clockface'

// Actions
import {setDropdownOrgID as setDropdownOrgIDAction} from 'src/tasks/actions'

// Constants
import {defaultAllOrgs} from 'src/tasks/constants'

// Types
import {Organization, AppState} from 'src/types'

interface ConnectedDispatchProps {
  setDropdownOrgID: typeof setDropdownOrgIDAction
}

interface ConnectedStateProps {
  orgs: Organization[]
  dropdownOrgID: string
}

type Props = ConnectedDispatchProps & ConnectedStateProps

class TasksOrgDropdown extends PureComponent<Props> {
  public render() {
    const {setDropdownOrgID} = this.props

    return (
      <Dropdown
        selectedID={this.selectedID}
        onChange={setDropdownOrgID}
        widthPixels={150}
      >
        {this.allOrgs.map(({id, name}) => (
          <Dropdown.Item key={id} value={id} id={id}>
            {name}
          </Dropdown.Item>
        ))}
      </Dropdown>
    )
  }

  private get selectedID(): string {
    const {dropdownOrgID} = this.props
    if (!dropdownOrgID) {
      return this.allOrgs[0].id
    }
    return dropdownOrgID
  }

  private get allOrgs(): Array<Partial<Organization>> {
    const {orgs} = this.props
    return [defaultAllOrgs, ...orgs]
  }
}

const mstp = ({
  tasks: {dropdownOrgID},
  orgs: {items},
}: AppState): ConnectedStateProps => {
  return {
    orgs: items,
    dropdownOrgID,
  }
}

const mdtp: ConnectedDispatchProps = {
  setDropdownOrgID: setDropdownOrgIDAction,
}

export default connect<ConnectedStateProps, ConnectedDispatchProps>(
  mstp,
  mdtp
)(TasksOrgDropdown)
