import React, {PropTypes} from 'react';
import classNames from 'classnames';

const QueryTabItem = React.createClass({
  propTypes: {
    isActive: PropTypes.bool,
    query: PropTypes.shape({
      rawText: PropTypes.string,
    }).isRequired,
    onSelect: PropTypes.func.isRequired,
    onDelete: PropTypes.func.isRequired,
  },

  handleSelect() {
    this.props.onSelect(this.props.query);
  },

  handleDelete(e) {
    e.stopPropagation();
    this.props.onDelete(this.props.query);
  },

  render() {
    return (
      <div className={classNames('explorer--tab', {active: this.props.isActive})} onClick={this.handleSelect}>
        <span className="explorer--tab-label">{this.props.query.rawText ? 'Raw Text' : 'Query'}</span>
        <span className="explorer--tab-delete" onClick={this.handleDelete}></span>
      </div>
    );
  },
});

export default QueryTabItem;
