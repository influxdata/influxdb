import React, {PropTypes} from 'react';
import FlashMessages from 'shared/components/FlashMessages';
import HostsTable from '../components/HostsTable';

export const HostsPage = React.createClass({
  propTypes: {
    sources: PropTypes.arrayOf(React.PropTypes.object),
  },

  render() {
    const {sources} = this.props;

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
                  <HostsTable hosts={sources} />
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
