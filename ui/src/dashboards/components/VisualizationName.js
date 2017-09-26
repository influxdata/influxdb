import React, {Component, PropTypes} from 'react'

class VisualizationName extends Component {
  constructor(props) {
    super(props)

    this.state = {
      reset: false,
    }
  }

  handleInputBlur = reset => e => {
    this.props.onCellRename(reset ? this.props.defaultName : e.target.value)
    this.setState({reset: false})
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

  render() {
    const {defaultName} = this.props
    const {reset} = this.state

    return (
      <div className="graph-heading">
        <input
          type="text"
          className="form-control input-md"
          defaultValue={defaultName}
          onBlur={this.handleInputBlur(reset)}
          onKeyDown={this.handleKeyDown}
          placeholder="Name this Cell..."
          ref={r => (this.inputRef = r)}
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
