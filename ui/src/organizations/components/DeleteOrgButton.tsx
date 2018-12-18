// Libraries
import React, {Component} from 'react'

// Components
import {Button, ComponentColor, ComponentSize} from 'src/clockface'

// Types
import {Organization} from 'src/api'

interface Props {
  org: Organization
  onDeleteOrg: (org: Organization) => void
}

class DeleteOrgButton extends Component<Props> {
  public render() {
    return (
      <Button
        size={ComponentSize.ExtraSmall}
        color={ComponentColor.Danger}
        text="Delete"
        onClick={this.handleDeleteOrg}
      />
    )
  }

  private handleDeleteOrg = () => {
    const {org, onDeleteOrg} = this.props
    onDeleteOrg(org)
  }
}

export default DeleteOrgButton
