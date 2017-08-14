import React, {PropTypes} from 'react'
import classnames from 'classnames'

const QueryMakerTab = React.createClass({
  propTypes: {
    isActive: PropTypes.bool,
    query: PropTypes.shape({
      rawText: PropTypes.string,
    }).isRequired,
    onSelect: PropTypes.func.isRequired,
    onDelete: PropTypes.func.isRequired,
    queryTabText: PropTypes.string,
    queryIndex: PropTypes.number,
  },

  handleSelect() {
    this.props.onSelect(this.props.queryIndex)
  },

  handleDelete(e) {
    e.stopPropagation()
    this.props.onDelete(this.props.queryIndex)
  },

  render() {
    return (
      <div
        className={classnames('query-maker--tab', {
          active: this.props.isActive,
        })}
        onClick={this.handleSelect}
      >
        <label>
          {this.props.queryTabText}
        </label>
        <span
          className="query-maker--delete"
          onClick={this.handleDelete}
          data-test="query-maker-delete"
        />
      </div>
    )
  },
})

export default QueryMakerTab
