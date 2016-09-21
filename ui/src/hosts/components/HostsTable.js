import React, {PropTypes} from 'react';

const HostsTable = React.createClass({
  propTypes: {
    hosts: PropTypes.arrayOf(React.PropTypes.object),
  },

  render() {
    const {hosts} = this.props;

    return (
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
            hosts.map(({name, id}) => {
              return (
                <tr key={id}>
                  <td><a href={`/hosts/${id}`}>{name}</a></td>
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
    );
  },
});

export default HostsTable;
