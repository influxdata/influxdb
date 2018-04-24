import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {renameCell} from 'src/dashboards/actions/cellEditorOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class VisualizationName extends Component {
  constructor(props) {
    super(props)

    this.state = {
      workingName: props.name,
    }
  }
  handleChange = e => {
    this.setState({workingName: e.target.value})
  }

  handleBlur = () => {
    const {handleRenameCell} = this.props
    const {workingName} = this.state

    handleRenameCell(workingName)
  }

  handleKeyDown = e => {
    if (e.key === 'Enter' || e.key === 'Escape') {
      e.target.blur()
    }
  }

  handleFocus = e => {
    e.target.select()
  }

  render() {
    const {workingName} = this.state

    return (
      <div className="graph-heading">
        <input
          type="text"
          className="form-control input-sm"
          value={workingName}
          onChange={this.handleChange}
          onFocus={this.handleFocus}
          onBlur={this.handleBlur}
          onKeyDown={this.handleKeyDown}
          placeholder="Name this Cell..."
          spellCheck={false}
        />
      </div>
    )
  }
}

const {string, func} = PropTypes

VisualizationName.propTypes = {
  name: string.isRequired,
  handleRenameCell: func,
}

const mapStateToProps = ({
  cellEditorOverlay: {
    cell: {name},
  },
}) => ({
  name,
})

const mapDispatchToProps = dispatch => ({
  handleRenameCell: bindActionCreators(renameCell, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(VisualizationName)
