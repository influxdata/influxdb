import React, {PropTypes} from 'react';
// TODO: move this to a higher level package than chronograf?
import AutoRefresh from 'shared/components/AutoRefresh';
import LineGraph from 'shared/components/LineGraph';

const RefreshingLineGraph = AutoRefresh(LineGraph);

export const HostPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    params: PropTypes.shape({
      hostID: PropTypes.string.isRequired,
    }).isRequired,
  },

  render() {
    const autoRefreshMs = 15000;
    const source = this.props.source.links.proxy;
    const hostID = this.props.params.hostID;
    const queries = [
      {
        text: `SELECT "usage_user" FROM "telegraf".."cpu" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: 'CPU',
      },
      {
        text: `SELECT "used_percent" FROM "telegraf".."mem" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: "Memory",
      },
      {
        text: `SELECT "load1" FROM "telegraf".."system" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: "Load",
      },
      {
        text: `SELECT "bytes_recv", "bytes_sent" FROM "telegraf".."net" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: "Network",
      },
      {
        text: `SELECT "io_time" FROM "telegraf".."diskio" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: "Disk IO",
      },
      {
        text: `SELECT "used_percent" FROM "telegraf".."disk" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: "Disk Usage",
      },
    ];

    return (
      <div className="host-dashboard hosts-page">
        <div className="enterprise-header hosts-dashboard-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>{hostID}</h1>
            </div>
            <div className="enterprise-header__right">
              <p>Uptime: <strong>2d 4h 33m</strong></p>
            </div>
          </div>
        </div>
        <div className="container-fluid hosts-dashboard">
          <div className="row">
            {
              queries.map((query) => {
                const q = Object.assign({}, query, {host: source});
                return (
                  <div className="col-xs-12 col-sm-6 col-lg-4" key={q.name}>
                    <h2 className="hosts-graph-heading">{q.name}</h2>
                    <div className="hosts-graph graph-panel__graph-container">
                      <RefreshingLineGraph
                        queries={[q]}
                        autoRefresh={autoRefreshMs}
                      />
                    </div>
                  </div>
                );
              })
            }
          </div>
        </div>
      </div>
    );
  },
});

export default HostPage;
