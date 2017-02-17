import React, {PropTypes} from 'react';
import moment from 'moment';
import {withRouter} from 'react-router';
import TimeRangeDropdown from '../../shared/components/TimeRangeDropdown';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';

const Header = React.createClass({
  propTypes: {
    timeRange: PropTypes.shape({
      upper: PropTypes.string,
      lower: PropTypes.string,
    }).isRequired,
    actions: PropTypes.shape({
      setTimeRange: PropTypes.func.isRequired,
    }),
  },

  contextTypes: {
    source: PropTypes.shape({
      name: PropTypes.string,
    }),
  },

  handleChooseTimeRange(bounds) {
    this.props.actions.setTimeRange(bounds);
  },

  findSelected({upper, lower}) {
    if (upper && lower) {
      const format = (t) => moment(t.replace(/\'/g, '')).format('YYYY-MM-DD HH:mm');
      return `${format(lower)} - ${format(upper)}`;
    }

    const selected = timeRanges.find((range) => range.queryValue === lower);
    return selected ? selected.inputValue : 'Custom';
  },

  render() {
    const {timeRange} = this.props;

    return (
      <div className="page-header">
        <div className="page-header__container">
          <div className="page-header__left">
            <h1>Explorer</h1>
          </div>
          <div className="page-header__right">
            <h1>Source:</h1>
            <div className="source-indicator">
              <span className="icon cpu"></span>
              {this.context.source.name}
            </div>
            <TimeRangeDropdown onChooseTimeRange={this.handleChooseTimeRange} selected={this.findSelected(timeRange)} />
          </div>
        </div>
      </div>
    );
  },
});

export default withRouter(Header);
