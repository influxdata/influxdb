import React, {PropTypes} from 'react';
import classNames from 'classnames';
import _ from 'lodash';

import {showMeasurements} from 'shared/apis/metaQuery';
import showMeasurementsParser from 'shared/parsing/showMeasurements';

const MeasurementList = React.createClass({
  propTypes: {
    query: PropTypes.shape({
      database: PropTypes.string,
      measurement: PropTypes.string,
    }).isRequired,
    onChooseMeasurement: PropTypes.func.isRequired,
  },

  contextTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      measurements: [],
      filterText: "",
    };
  },

  componentDidMount() {
    if (!this.props.query.database) {
      return;
    }

    const {source} = this.context;
    const proxy = source.links.proxy;
    showMeasurements(proxy, this.props.query.database).then((resp) => {
      const {errors, measurementSets} = showMeasurementsParser(resp.data);
      if (errors.length) {
        // TODO: display errors in the UI.
        return console.error('InfluxDB returned error(s): ', errors); // eslint-disable-line no-console
      }

      this.setState({
        measurements: measurementSets[0].measurements,
      });
    });
  },

  handleFilterText(e) {
    e.stopPropagation();
    this.setState({
      filterText: this.refs.filterText.value,
    });
  },

  handleEscape(e) {
    if (e.key !== 'Escape') {
      return;
    }

    e.stopPropagation();
    this.setState({
      filterText: '',
    });
  },

  render() {
    return (
      <div className="measurement-list">
        <div className="query-editor__list-header">
          <input className="query-editor__filter" ref="filterText" placeholder="Filter Measurements..." type="text" value={this.state.filterText} onChange={this.handleFilterText} onKeyUp={this.handleEscape} />
          <span className="icon search"></span>
        </div>
        {this.renderList()}
      </div>
    );
  },

  renderList() {
    if (!this.props.query.database) {
      return <div className="query-editor__empty">No database selected.</div>;
    }

    const measurements = this.state.measurements.filter((m) => m.match(this.state.filterText));

    return (
      <ul className="query-editor__list">
        {measurements.map((measurement) => {
          const isActive = measurement === this.props.query.measurement;
          return (
            <li className={classNames('query-editor__list-item query-editor__list-radio', {active: isActive})} key={measurement} onClick={_.wrap(measurement, this.props.onChooseMeasurement)}>{measurement}</li>
          );
        })}
      </ul>
    );
  },
});

export default MeasurementList;
