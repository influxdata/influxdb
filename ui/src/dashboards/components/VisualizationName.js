import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {NEW_DEFAULT_DASHBOARD_CELL} from 'src/dashboards/constants/index'
import {renameCell} from 'src/dashboards/actions/cellEditorOverlay'

class VisualizationName extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isEditing: false,
    }
  }

  handleInputClick = () => {
    this.setState({isEditing: true})
  }

  handleCancel = () => {
    this.setState({
      isEditing: false,
    })
  }

  handleInputBlur = () => {
    this.setState({isEditing: false})
  }

  handleKeyDown = e => {
    const {handleRenameCell} = this.props

    if (e.key === 'Enter') {
      handleRenameCell(e.target.value)
      this.handleInputBlur(e)
    }
    if (e.key === 'Escape') {
      this.handleInputBlur(e)
    }
  }

  handleFocus = e => {
    e.target.select()
  }

  render() {
    const {name} = this.props
    const {isEditing} = this.state
    const graphNameClass =
      name === NEW_DEFAULT_DASHBOARD_CELL.name
        ? 'graph-name graph-name__untitled'
        : 'graph-name'

    return (
      <div className="graph-heading">
        {isEditing
          ? <input
              type="text"
              className="form-control input-sm"
              defaultValue={name}
              onBlur={this.handleInputBlur}
              onKeyDown={this.handleKeyDown}
              autoFocus={true}
              onFocus={this.handleFocus}
              placeholder="Name this Cell..."
            />
          : <div className={graphNameClass} onClick={this.handleInputClick}>
              {name}
            </div>}
      </div>
    )
  }
}

const {string, func} = PropTypes

VisualizationName.propTypes = {
  name: string.isRequired,
  handleRenameCell: func,
}

const mapStateToProps = ({cellEditorOverlay: {cell: {name}}}) => ({
  name,
})

const mapDispatchToProps = dispatch => ({
  handleRenameCell: bindActionCreators(renameCell, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(VisualizationName)
