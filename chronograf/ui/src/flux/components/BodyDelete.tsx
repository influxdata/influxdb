import React, {PureComponent} from 'react'
import ConfirmButton from 'src/shared/components/ConfirmButton'

type BodyType = 'variable' | 'query'

interface Props {
  bodyID: string
  type?: BodyType
  onDeleteBody: (bodyID: string) => void
}

class BodyDelete extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    type: 'query',
  }

  public render() {
    const {type} = this.props

    if (type === 'variable') {
      return (
        <button
          className="btn btn-sm btn-square btn-danger"
          title="Delete Variable"
          onClick={this.handleDelete}
        >
          <span className="icon remove" />
        </button>
      )
    }

    return (
      <ConfirmButton
        icon="trash"
        type="btn-danger"
        confirmText="Delete Query"
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
