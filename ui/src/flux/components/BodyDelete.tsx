import React, {PureComponent} from 'react'
import ConfirmButton from 'src/shared/components/ConfirmButton'

interface Props {
  bodyID: string
  onDeleteBody: (bodyID: string) => void
}

class BodyDelete extends PureComponent<Props> {
  public render() {
    return (
      <ConfirmButton
        icon="trash"
        type="btn-danger"
        confirmText="Delete this query"
        square={true}
        confirmAction={this.handleDelete}
        position="right"
      />
    )
  }

  private handleDelete = (): void => {
    this.props.onDeleteBody(this.props.bodyID)
  }
}

export default BodyDelete
