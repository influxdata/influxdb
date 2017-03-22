import React, {PropTypes} from 'react';
import {Link} from 'react-router';
import SourceIndicator from '../../shared/components/SourceIndicator';

import {getDashboards} from '../apis';

const DashboardsPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      type: PropTypes.string,
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
      telegraf: PropTypes.string.isRequired,
    }),
    addFlashMessage: PropTypes.func,
  },

  getInitialState() {
    return {
      dashboards: [],
      waiting: true,
    };
  },

  componentDidMount() {
    getDashboards().then((resp) => {
      this.setState({
        dashboards: resp.data.dashboards,
        waiting: false,
      });
    });
  },

  render() {
    const dashboardLink = `/sources/${this.props.source.id}`;
    let tableHeader;
    if (this.state.waiting) {
      tableHeader = "Loading Dashboards...";
    } else if (this.state.dashboards.length === 0) {
      tableHeader = "1 Dashboard";
    } else {
      tableHeader = `${this.state.dashboards.length + 1} Dashboards`;
    }

    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>
                Dashboards
              </h1>
            </div>
            <div className="page-header__right">
              <SourceIndicator sourceName={this.props.source.name} />
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-12">
                <div className="panel panel-minimal">
                  <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                    <h2 className="panel-title">{tableHeader}</h2>
                    <button className="btn btn-sm btn-primary">Create Dashboard</button>
                  </div>
                  <div className="panel-body">
                    <table className="table v-center">
                      <thead>
                        <tr>
                          <th>Name</th>
                        </tr>
                      </thead>
                      <tbody>
                          {
                            this.state.dashboards.map((dashboard) => {
                              return (
                                <tr key={dashboard.id}>
                                  <td className="monotype">
                                    <Link to={`${dashboardLink}/dashboards/${dashboard.id}`}>
                                      {dashboard.name}
                                    </Link>
                                  </td>
                                </tr>
                              );
                            })
                          }
                          <tr>
                            <td className="monotype">
                              <Link to={`${dashboardLink}/kubernetes`}>
                                {'Kubernetes'}
                              </Link>
                            </td>
                          </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default DashboardsPage;
