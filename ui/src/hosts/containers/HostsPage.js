import React, {PropTypes} from 'react';
import FlashMessages from 'shared/components/FlashMessages';

export const HostsPage = React.createClass({
  propTypes: {
    dataNodes: PropTypes.arrayOf(React.PropTypes.object),
  },

  render() {
    const {dataNodes} = this.props;

    return (
      <div className="hosts">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                Hosts
              </h1>
            </div>
          </div>
        </div>

        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <div className="panel panel-minimal">
                <div className="panel-body">
                  <table className="table v-center">
                    <thead>
                      <tr>
                        <th>Hostname</th>
                        <th>Status</th>
                        <th>CPU</th>
                        <th>Load</th>
                        <th>Apps</th>
                      </tr>
                    </thead>
                    <tbody>
                      {
                        dataNodes.map(({name, id}) => {
                          return (
                            <tr key={id}>
                              <td><a href={`/host/${id}`}>{name}</a></td>
                              <td>UP</td>
                              <td>98%</td>
                              <td>1.12</td>
                              <td>influxdb, ntp, system</td>
                            </tr>
                          );
                        })
                      }
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },

});

export default FlashMessages(HostsPage);
