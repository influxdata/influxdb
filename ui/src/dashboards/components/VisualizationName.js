import React, {Component, PropTypes} from 'react'

class VisualizationName extends Component {
  constructor(props) {
    super(props)
  }

  handleInputBlur = e => {
    this.props.onCellRename(e.target.value)
  }

  render() {
    const {defaultName} = this.props

    return (
      <div className="graph-heading">
        <input
          type="text"
          className="form-control input-md"
          defaultValue={defaultName}
          onBlur={this.handleInputBlur}
          placeholder="Name this Cell..."
        />
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
