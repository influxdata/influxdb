import React, {Component, PropTypes} from 'react'

class FilterBar extends Component {
  constructor(props) {
    super(props)

    this.state = {
      filterText: '',
    }
  }

  handleText = e => {
    this.setState(
      {filterText: e.target.value},
      this.props.onFilter(e.target.value)
    )
  }

  componentWillUnmount() {
    this.props.onFilter('')
  }

  render() {
    const {type, isEditing, onClickCreate} = this.props
    const placeholderText = type.replace(/\w\S*/g, function(txt) {
      return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
    })
    return (
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <div className="users__search-widget input-group admin__search-widget">
          <input
            type="text"
            className="form-control input-sm"
            placeholder={`Filter ${placeholderText}...`}
            value={this.state.filterText}
            onChange={this.handleText}
          />
          <div className="input-group-addon">
            <span className="icon search" aria-hidden="true" />
          </div>
        </div>
        <button
          className="btn btn-sm btn-primary"
          disabled={isEditing}
          onClick={onClickCreate(type)}
        >
          Create {placeholderText.substring(0, placeholderText.length - 1)}
        </button>
      </div>
    )
  }
}

const {bool, func, string} = PropTypes

FilterBar.propTypes = {
  onFilter: func.isRequired,
  type: string,
  isEditing: bool,
  onClickCreate: func,
}

export default FilterBar
