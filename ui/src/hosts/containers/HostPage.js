import React, {PropTypes} from 'react';
// TODO: move this to a higher level package than chronograf?
import AutoRefresh from 'shared/components/AutoRefresh';
import LineGraph from 'shared/components/LineGraph';
import FlashMessages from 'shared/components/FlashMessages';

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
    const queries = [
      {
        text: `SELECT "usage_user" FROM "telegraf"."default"."cpu" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: 'CPU',
      },
      {
        text: `SELECT "used_percent" FROM "telegraf"."default"."mem" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: "Memory",
      },
      {
        text: `SELECT "load1" FROM "telegraf"."default"."system" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: "Load",
      },
      {
        text: `SELECT "bytes_recv", "bytes_sent" FROM "telegraf"."default"."net" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: "Network",
      },
      {
        text: `SELECT "io_time" FROM "telegraf"."default"."diskio" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: "Disk IO",
      },
      {
        text: `SELECT "used_percent" FROM "telegraf"."default"."disk" WHERE host = '${this.props.params.hostID}' AND time > now() - 15m`,
        name: "Disk usage",
      },
    ];

    return (
      <div className="container-fluid">
        <div className="row">
          {
          queries.map((query) => {
            const q = Object.assign({}, query, {host: source});
            return (
              <div className="col-md-4 graph-panel__graph-container" key={q.name}>
                <h2>{q.name}</h2>
                <RefreshingLineGraph
                  queries={[q]}
                  autoRefresh={autoRefreshMs}
                />
              </div>
            );
          })
        }
        </div>
      </div>
    );
  },
});

export default FlashMessages(HostPage);
