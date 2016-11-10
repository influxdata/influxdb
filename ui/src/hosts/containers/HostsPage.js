import React, {PropTypes} from 'react';
import _ from 'lodash';
import HostsTable from '../components/HostsTable';
import {getCpuAndLoadForHosts, getMappings, getAppsForHosts} from '../apis';

export const HostsPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      type: PropTypes.string, // 'influx-enterprise'
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }),
    addFlashMessage: PropTypes.func,
  },

  getInitialState() {
    return {
      hosts: {},
    };
  },

  componentDidMount() {
    const {source, addFlashMessage} = this.props;
    Promise.all([
      getCpuAndLoadForHosts(source.links.proxy),
      getMappings(),
    ]).then(([hosts, {data: {mappings}}]) => {
      this.setState({hosts});
      getAppsForHosts(source.links.proxy, hosts, mappings).then((newHosts) => {
        this.setState({hosts: newHosts});
      }).catch(() => {
        addFlashMessage({type: 'error', text: 'Unable to get apps for hosts'});
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

export default HostsPage;
