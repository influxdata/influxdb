import React, {PropTypes} from 'react';

export default React.createClass({
  propTypes: {
    databases: PropTypes.array.isRequired, // eslint-disable-line react/forbid-prop-types
    selectedDatabase: PropTypes.string.isRequired,
    onChooseDatabase: PropTypes.func.isRequired,
  },

  render() {
    return (
      <div className="enterprise-header">
        <div className="enterprise-header__container">
          <div className="enterprise-header__left">
            <div className="dropdown minimal-dropdown">
              <button className="dropdown-toggle" type="button" id="dropdownMenu1" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true">
                {this.props.selectedDatabase}
                <span className="caret" />
              </button>
              <ul className="dropdown-menu" aria-labelledby="dropdownMenu1">
                {this.props.databases.map((d) => {
                  return <li key={d} onClick={() => this.props.onChooseDatabase(d)}><a href="#">{d}</a></li>;
                })}
              </ul>
            </div>
          </div>
          <div className="enterprise-header__right">
            <button className="btn btn-sm btn-primary" data-toggle="modal" data-target="#rpModal">Create Retention Policy</button>
          </div>
        </div>
      </div>
    );
  },
});
