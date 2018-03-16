import React, {PureComponent} from 'react'

import InputClickToEdit from 'src/shared/components/InputClickToEdit'

type Column = {
  internalName: string
  displayName: string
  visible: boolean
}

interface Props {
  internalName: string
  displayName: string
  visible: boolean
  onColumnUpdate: (column: Column) => void
}

class GraphOptionsCustomizableColumn extends PureComponent<Props, {}> {
  constructor(props) {
    super(props)

    this.handleColumnRename = this.handleColumnRename.bind(this)
    this.handleToggleVisible = this.handleToggleVisible.bind(this)
  }

  handleColumnRename(rename: string) {
    const {onColumnUpdate, internalName, visible} = this.props
    onColumnUpdate({internalName, displayName: rename, visible})
  }

  handleToggleVisible() {
    const {onColumnUpdate, internalName, displayName, visible} = this.props
    onColumnUpdate({internalName, displayName, visible: !visible})
  }

  render() {
    const {internalName, displayName, visible} = this.props

    return (
      <div className="column-controls--section">
        <div
          className={
            visible ? 'column-controls--label' : 'column-controls--label-hidden'
          }
        >
          <span className="icon eye" onClick={this.handleToggleVisible} />
          {internalName}
        </div>
        <InputClickToEdit
          value={displayName}
          wrapperClass="column-controls-input"
          onBlur={this.handleColumnRename}
          placeholder="Rename..."
          appearAsNormalInput={true}
          disabled={!visible}
        />
      </div>
    )
  }
}

export default GraphOptionsCustomizableColumn
