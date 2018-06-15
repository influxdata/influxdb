import React, {PureComponent} from 'react'

interface Props {
  bodyID: string
  onDeleteBody: (bodyID: string) => void
}

class BodyDelete extends PureComponent<Props> {
  public render() {
    return (
      <button className="btn btn-xs btn-danger" onClick={this.handleDelete}>
        Delete
      </button>
    )
  }

  private handleDelete = (): void => {
    this.props.onDeleteBody(this.props.bodyID)
  }
}

export default BodyDelete
