// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Dropdown} from 'src/clockface'

// Types
import {Organization} from 'src/types/v2'

interface OwnProps {
  selectedOrgID: string
  onSelectOrg: (orgID: string) => void
}

interface StateProps {
  orgs: Organization[]
}

type Props = OwnProps & StateProps

class OrgDropdown extends PureComponent<Props> {
  public render() {
    const {orgs, onSelectOrg} = this.props

    return (
      <Dropdown
        selectedID={this.selectedID}
        onChange={onSelectOrg}
        widthPixels={200}
      >
        {orgs.map(({id, name}) => (
          <Dropdown.Item key={id} value={id} id={id}>
            {name}
          </Dropdown.Item>
        ))}
      </Dropdown>
    )
  }

  private get selectedID(): string {
    const {selectedOrgID} = this.props
    return selectedOrgID
  }
}

const mstp = ({orgs}): StateProps => {
  return {
    orgs,
  }
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  null
)(OrgDropdown)
