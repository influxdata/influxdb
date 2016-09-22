import React, {PropTypes} from 'react';
import Reactable from 'reactable';

const HostsTable = React.createClass({
  propTypes: {
    hosts: PropTypes.arrayOf(React.PropTypes.object),
  },

  render() {
    const {hosts} = this.props;
    const {Table, Thead, Tr, Td, Th} = Reactable;


    return (
      <Table sortable={true} className="table v-center">
        <Thead>
          <Th column="hostname">Hostname</Th>
          <Th column="status">Status</Th>
          <Th column="cpu">CPU</Th>
          <Th column="load">Load</Th>
          <Th column="apps">Apps</Th>
        </Thead>
        {
          hosts.map(({name, id}) => {
            return (
              <Tr key={id}>
                <Td column="hostname"><a href={`/hosts/${id}`}>{name}</a></Td>
                <Td column="status">UP</Td>
                <Td column="cpu">98%</Td>
                <Td column="load">1.12</Td>
                <Td column="apps">influxdb, ntp, system</Td>
              </Tr>
            );
          })
        }
      </Table>
    );
  },
});

export default HostsTable;
