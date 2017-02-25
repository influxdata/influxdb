import React from 'react';
import classnames from 'classnames';
import OnClickOutside from 'shared/components/OnClickOutside';

import autoRefreshValues from 'hson!../data/autoRefreshValues.hson';

const AutoRefreshDropdown = React.createClass({
  autobind: false,

  propTypes: {
    selected: React.PropTypes.string.isRequired,
    onChoose: React.PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      isOpen: false,
    };
  },

  handleClickOutside() {
    this.setState({isOpen: false});
  },

  handleSelection(params) {
    const {seconds, menuOption} = params;
    this.props.onChoose({seconds});
    this.setState({isOpen: false});
  },

  toggleMenu() {
    this.setState({isOpen: !this.state.isOpen});
  },

  render() {
    const self = this;
    const {selected} = self.props;
    const {isOpen} = self.state;

    return (
      <div className="dropdown time-range-dropdown">
        <div className="btn btn-sm btn-info dropdown-toggle" onClick={() => self.toggleMenu()}>
          <span className="icon refresh"></span>
          <span className="selected-time-range">{selected}</span>
          <span className="caret" />
        </div>
        <ul className={classnames("dropdown-menu", {show: isOpen})}>
          <li className="dropdown-header">Time Range</li>
          {autoRefreshValues.map((item) => {
            return (
              <li key={item.menuOption}>
                <a href="#" onClick={() => self.handleSelection(item)}>
                  {item.menuOption}
                </a>
              </li>
            );
          })}
        </ul>
      </div>
    );
  },
});

export default OnClickOutside(AutoRefreshDropdown);
