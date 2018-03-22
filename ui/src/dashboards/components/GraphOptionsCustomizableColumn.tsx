import React, {PureComponent} from 'react'

import InputClickToEdit from 'src/shared/components/InputClickToEdit'

interface Column {
  internalName: string
  displayName: string
}

interface Props {
  internalName: string
  displayName: string
  onColumnRename: (column: Column) => void
}

class GraphOptionsCustomizableColumn extends PureComponent<Props, {}> {
  constructor(props) {
    super(props)

    this.handleColumnRename = this.handleColumnRename.bind(this)
  }

  public handleColumnRename(rename) {
    const {onColumnRename, internalName} = this.props
    onColumnRename({internalName, displayName: rename})
  }

  public render() {
    const {internalName, displayName} = this.props

    return (
      <div className="column-controls--section">
        <div className="column-controls--label">
          {internalName}
        </div>
        <InputClickToEdit
          value={displayName}
          wrapperClass="column-controls-input"
          onBlur={this.handleColumnRename}
          placeholder="Rename..."
          appearAsNormalInput={true}
        />
      </div>
    )
  }
}

export default GraphOptionsCustomizableColumn
