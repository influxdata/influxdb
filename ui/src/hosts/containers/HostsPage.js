import React, {PropTypes} from 'react';
import _ from 'lodash';
import FlashMessages from 'shared/components/FlashMessages';
import HostsTable from '../components/HostsTable';
import {getCpuAndLoadForHosts} from '../apis';
import {proxy} from 'utils/queryUrlGenerator';

export const HostsPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      type: PropTypes.string, // 'influx-enterprise'
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      hosts: {},
    };
  },

  componentDidMount() {
    getCpuAndLoadForHosts(this.props.source.links.proxy).then((hosts) => {
      this.setState({hosts});
      proxy({
        source: this.props.source.links.proxy,
        query: `show series from /influxdb$|docker$/`,
        db: 'telegraf',
      }).then((resp) => {
        const newHosts = Object.assign({}, hosts);
        resp.data.results[0].series[0].values.forEach((vals) => {
          const val = vals[0];
          const matches = val.match(/(\w*),.*,host=([^,]*)/);
          if (!matches || matches.length !== 3) {
            return;
          }
          const [_blah, app, host] = matches;
          if (!newHosts[host]) {
            return;
          }
          if (!newHosts[host].apps) {
            newHosts[host].apps = [];
          }
          newHosts[host].apps = _.uniq(newHosts[host].apps.concat(app));
        });

        this.setState({hosts: newHosts});
      });
    }).catch(() => {
      this.props.addFlashMessage({
        type: 'error',
        text: `There was an error finding hosts. Check that your server is running.`,
      });
    });
  },

  render() {
    return (
      <div className="hosts hosts-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                Host List
              </h1>
            </div>
          </div>
        </div>

        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <HostsTable source={this.props.source} hosts={_.values(this.state.hosts)} />
            </div>
          </div>
        </div>
      </div>
    );
  },

});

export default FlashMessages(HostsPage);
