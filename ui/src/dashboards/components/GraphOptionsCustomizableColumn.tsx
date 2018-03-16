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
  onColumnRename: (column: Column) => void
}

class GraphOptionsCustomizableColumn extends PureComponent<Props, {}> {
  constructor(props) {
    super(props)

    this.handleColumnRename = this.handleColumnRename.bind(this)
    this.handleToggleVisible = this.handleToggleVisible.bind(this)
  }

  handleColumnRename(rename: string) {
    const {onColumnRename, internalName, visible} = this.props
    onColumnRename({internalName, displayName: rename, visible})
  }

  handleToggleVisible() {
    const {onColumnRename, internalName, displayName, visible} = this.props
    onColumnRename({internalName, displayName, visible: !visible})
  }

  render() {
    const {internalName, displayName, visible} = this.props
    console.log('VISIBLE:', visible)

    return (
      <div className="column-controls--section">
        <div
          className="column-controls--label"
          onClick={this.handleToggleVisible}
        >
          <span className="icon eye" />
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
