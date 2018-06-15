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
    return (
      <ConfirmButton
        icon="trash"
        type="btn-danger"
        confirmText={this.confirmText}
        square={true}
        confirmAction={this.handleDelete}
        position="right"
      />
    )
  }

  private handleDelete = (): void => {
    this.props.onDeleteBody(this.props.bodyID)
  }

  private get confirmText(): string {
    const {type} = this.props

    return `Delete this ${type}`
  }
}

export default BodyDelete
