import React, {PropTypes} from 'react';
import _ from 'lodash';
import HostsTable from '../components/HostsTable';
import {getCpuAndLoadForHosts, getMappings, getAppsForHosts, getHostStatus} from '../apis';

export const HostsPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      type: PropTypes.string, // 'influx-enterprise'
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
      telegraf: PropTypes.string.isRequired,
    }),
    addFlashMessage: PropTypes.func,
  },

  getInitialState() {
    return {
      hosts: {},
      up: {},
    };
  },

  componentDidMount() {
    const {source, addFlashMessage} = this.props;
    Promise.all([
      getCpuAndLoadForHosts(source.links.proxy, source.telegraf),
      getMappings(),
      getHostStatus(source.links.proxy, source.telegraf),
    ]).then(([hosts, {data: {mappings}}, up]) => {
      this.setState({hosts, up});
      getAppsForHosts(source.links.proxy, hosts, mappings, source.telegraf).then((newHosts) => {
        this.setState({hosts: newHosts});
      }).catch(() => {
        addFlashMessage({type: 'error', text: 'Unable to get apps for hosts'});
      });
    }).catch((reason) => {
      // TODO: this isn't reachable at the moment, because getCpuAndLoadForHosts doesn't fail when it should.
      // (like with a bogus proxy link). We should provide better messaging to the user in this catch after that's fixed.
      console.error(reason); // eslint-disable-line no-console
    });
  },

  render() {
    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>
                Host List
              </h1>
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-12">
                <HostsTable source={this.props.source} hosts={_.values(this.state.hosts)} up={this.state.up} />
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default HostsPage;
