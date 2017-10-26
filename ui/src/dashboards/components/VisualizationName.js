import React, {Component, PropTypes} from 'react'

import {NEW_DEFAULT_DASHBOARD_CELL} from 'src/dashboards/constants/index'

class VisualizationName extends Component {
  constructor(props) {
    super(props)

    this.state = {
      reset: false,
      isEditing: false,
    }
  }

  handleInputBlur = reset => e => {
    this.props.onCellRename(reset ? this.props.defaultName : e.target.value)
    this.setState({reset: false, isEditing: false})
  }

  handleKeyDown = e => {
    if (e.key === 'Enter') {
      this.inputRef.blur()
    }
    if (e.key === 'Escape') {
      this.inputRef.value = this.props.defaultName
      this.setState({reset: true}, () => this.inputRef.blur())
    }
  }

  handleEditMode = () => {
    this.setState({isEditing: true})
  }

  handleFocus = e => {
    e.target.select()
  }

  render() {
    const {defaultName} = this.props
    const {reset, isEditing} = this.state
    const graphNameClass =
      defaultName === NEW_DEFAULT_DASHBOARD_CELL.name
        ? 'graph-name graph-name__untitled'
        : 'graph-name'

    return (
      <div className="graph-heading">
        {isEditing
          ? <input
              type="text"
              className="form-control input-sm"
              defaultValue={defaultName}
              onBlur={this.handleInputBlur(reset)}
              onKeyDown={this.handleKeyDown}
              placeholder="Name this Cell..."
              autoFocus={true}
              onFocus={this.handleFocus}
              ref={r => (this.inputRef = r)}
            />
          : <div className={graphNameClass} onClick={this.handleEditMode}>
              {defaultName}
            </div>}
      </div>
    )
  }
}

const {string, func} = PropTypes

VisualizationName.propTypes = {
  defaultName: string.isRequired,
  onCellRename: func,
}

export default VisualizationName
