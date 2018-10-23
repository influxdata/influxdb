// Libraries
import React, {Component} from 'react'

// Components
import {Button, ComponentColor, ComponentSize} from 'src/clockface'

// Types
import {Organization} from 'src/types/v2'

interface Props {
  org: Organization
  onDeleteOrg: (link: string) => void
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
    onDeleteOrg(org.links.self)
  }
}

export default DeleteOrgButton
