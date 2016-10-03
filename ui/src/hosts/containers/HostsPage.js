import React, {PropTypes} from 'react';
import _ from 'lodash';
import FlashMessages from 'shared/components/FlashMessages';
import HostsTable from '../components/HostsTable';
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
  },

  getInitialState() {
    return {
      hosts: [],
    };
  },

  componentDidMount() {
    proxy({
      source: this.props.source.links.proxy,
      query: `select mean(usage_user) from cpu where cpu = 'cpu-total' and time > now() - 10m group by host; select mean("load1") from "telegraf"."default"."system" where time > now() - 10m group by host`,
      db: 'telegraf',
    }).then((resp) => {
      const hosts = {};
      const precision = 100;
      resp.data.results[0].series.forEach((s) => {
        const meanIndex = s.columns.findIndex((col) => col === 'mean');
        hosts[s.tags.host] = {
          name: s.tags.host,
          cpu: (Math.round(s.values[0][meanIndex] * precision) / precision).toFixed(2),
        };
      });

      resp.data.results[1].series.forEach((s) => {
        const meanIndex = s.columns.findIndex((col) => col === 'mean');
        hosts[s.tags.host].load = (Math.round(s.values[0][meanIndex] * precision) / precision).toFixed(2);
      });

      this.setState({
        hosts: _.values(hosts),
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
              <HostsTable source={this.props.source} hosts={this.state.hosts} />
            </div>
          </div>
        </div>
      </div>
    );
  },

});

export default FlashMessages(HostsPage);
