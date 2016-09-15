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
      <div className={classNames('explorer__tab', {active: this.props.isActive})} onClick={this.handleSelect}>
        {this.props.query.rawText ? 'Raw Query' : 'Query'} <span className="explorer__tab-delete" onClick={this.handleDelete}><span className="icon remove"></span></span>
      </div>
    );
  },
});

export default QueryTabItem;
