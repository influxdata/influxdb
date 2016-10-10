import React, {PropTypes} from 'react';
import FlashMessages from 'shared/components/FlashMessages';
import HostsTable from '../components/HostsTable';
import {getCpuAndLoadForHosts} from '../apis';

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
      hosts: [],
    };
  },

  componentDidMount() {
    getCpuAndLoadForHosts(this.props.source.links.proxy).then((hosts) => {
      this.setState({hosts});
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
              <HostsTable source={this.props.source} hosts={this.state.hosts} />
            </div>
          </div>
        </div>
      </div>
    );
  },

});

export default FlashMessages(HostsPage);
