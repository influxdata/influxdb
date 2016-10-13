import React, {PropTypes} from 'react';
// TODO: move this to a higher level package than chronograf?
import FlashMessages from 'shared/components/FlashMessages';
import LayoutRenderer from '../components/LayoutRenderer';
import {fetchLayout} from '../apis';

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

  componentDidMount() {
    fetchLayout().then((layout) => {
      this.setState({layout});
    });
  },

  render() {
    const autoRefreshMs = 15000;
    const source = this.props.source.links.proxy;
    const hostID = this.props.params.hostID;

    const cells = [
      {
        id: "18aed9a7-dc83-406e-a4dc-40d53049541a",
        cells: [
          {
            queries: [
              {
                rp: "autogen",
                text: "select usage_user from cpu",
                database: "telegraf",
              },
            ],
            x: 0,
            h: 100,
            y: 0,
            w: 50,
            i: "used_percent",
            name: "Disk Used Percent",
          },
        ],
        measurement: "disk",
        link: {
          rel: "self",
          href: "/chronograf/v1/layouts/18aed9a7-dc83-406e-a4dc-40d53049541a",
        },
        app: "User Facing Application Name",
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
            <LayoutRenderer
              cells={cells[0].cells}
              autoRefreshMs={autoRefreshMs}
              source={source}
              host={this.props.params.hostID}
            />
          </div>
        </div>
      </div>
    );
  },
});

export default HostPage;
