import React, {Component} from 'react'
import PropTypes from 'prop-types'

import InputClickToEdit from 'shared/components/InputClickToEdit'

class GraphOptionsCustomizableColumn extends Component {
  constructor(props) {
    super(props)

    this.handleColumnRename = this.handleColumnRename.bind(this)
  }

  handleColumnRename(rename) {
    const {onColumnRename, internalName} = this.props
    onColumnRename({internalName, displayName: rename})
  }

  render() {
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

const {func, string} = PropTypes

GraphOptionsCustomizableColumn.propTypes = {
  internalName: string,
  displayName: string,
  onColumnRename: func,
}

export default GraphOptionsCustomizableColumn
