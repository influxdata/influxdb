import React from 'react';
import classnames from 'classnames';
import OnClickOutside from 'shared/components/OnClickOutside';

import timeRanges from 'hson!../data/timeRanges.hson';

const TimeRangeDropdown = React.createClass({
  autobind: false,

  propTypes: {
    selected: React.PropTypes.string.isRequired,
    onChooseTimeRange: React.PropTypes.func.isRequired,
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
    const {queryValue, menuOption} = params;
    if (menuOption.toLowerCase() === 'custom') {
      this.props.onChooseTimeRange({custom: true});
    } else {
      this.props.onChooseTimeRange({lower: queryValue, upper: null});
    }
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
          <span className="icon clock"></span>
          <span className="selected-time-range">{selected}</span>
          <span className="caret" />
        </div>
        <ul className={classnames("dropdown-menu", {show: isOpen})}>
          <li className="dropdown-header">Time Range</li>
          {timeRanges.map((item) => {
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

export default OnClickOutside(TimeRangeDropdown);
