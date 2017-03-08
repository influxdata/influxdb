import React, {Component, PropTypes} from 'react'

class FilterBar extends Component {
  constructor(props) {
    super(props)
    this.state = {
      filterText: '',
    }

    this.handleText = ::this.handleText
  }

  handleText(e) {
    this.setState(
      {filterText: e.target.value},
      this.props.onFilter(e.target.value)
    )
  }

  componentWillUnmount() {
    this.props.onFilter('')
  }

  render() {
    const {type, onClickCreate} = this.props
    return (
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <div className="users__search-widget input-group admin__search-widget">
          <input
            type="text"
            className="form-control"
            placeholder={`Filter ${type}...`}
            value={this.state.filterText}
            onChange={this.handleText}
          />
          <div className="input-group-addon">
            <span className="icon search" aria-hidden="true"></span>
          </div>
        </div>
        <a href="#" className="btn btn-primary" onClick={() => onClickCreate(type)}>Create {type}</a>
      </div>
    )
  }
}

const {
  func,
  string,
} = PropTypes

FilterBar.propTypes = {
  onFilter: func.isRequired,
  type: string,
  onClickCreate: func,
}

export default FilterBar
