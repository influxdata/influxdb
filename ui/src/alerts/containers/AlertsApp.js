import React, {PropTypes} from 'react';
import AlertsTable from '../components/AlertsTable';
import {getAlerts} from '../apis';
import _ from 'lodash';

// Kevin: because we were getting strange errors saying
// "Failed prop type: Required prop `source` was not specified in `AlertsApp`."
// Tim and I decided to make the source and addFlashMessage props not required.
// FIXME: figure out why that wasn't working
const AlertsApp = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      type: PropTypes.string, // 'influx-enterprise'
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }), // .isRequired,
    addFlashMessage: PropTypes.func, // .isRequired,
  },

  getInitialState() {
    return {
      alerts: [],
    };
  },

  componentDidMount() {
    return getAlerts(this.props.source.links.proxy).then((resp) => {
      const results = [];

      const alertSeries = _.get(resp, ['data', 'results', '0', 'series'], []);

      const timeIndex = alertSeries[0].columns.findIndex((col) => col === 'time');
      const hostIndex = alertSeries[0].columns.findIndex((col) => col === 'host');
      const valueIndex = alertSeries[0].columns.findIndex((col) => col === 'value');
      const levelIndex = alertSeries[0].columns.findIndex((col) => col === 'level');
      const nameIndex = alertSeries[0].columns.findIndex((col) => col === 'alert_name');

      alertSeries[0].values.forEach((s) => {
        results.push({
          time: `${s[timeIndex]}`,
          host: s[hostIndex],
          value: `${s[valueIndex]}`,
          level: s[levelIndex],
          name: `${s[nameIndex]}`,
        });
      });
      this.setState({alerts: results});
    });
  },

  render() {
    return (
      // I stole this from the Hosts page.
      // Perhaps we should create an abstraction?
      <div className="hosts hosts-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                Alerts
              </h1>
            </div>
          </div>
        </div>

        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <AlertsTable source={this.props.source} alerts={this.state.alerts} />
            </div>
          </div>
        </div>
      </div>
    );
  },

});

export default AlertsApp;
