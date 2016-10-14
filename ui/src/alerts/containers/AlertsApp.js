import React, {PropTypes} from 'react';
import AlertsTable from '../components/AlertsTable';
import {getAlerts} from '../apis';

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
    this.setState(
      { alerts:getAlerts() }
    );
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
