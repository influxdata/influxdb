import React from 'react';

import {getDashboards} from '../apis';

const DashboardsPage = React.createClass({
  getInitialState() {
    return {
      dashboards: [],
    };
  },

  componentDidMount() {
    getDashboards().then((resp) => {
      this.setState({
        dashboards: resp.data.dashboards,
      });
    });
  },

  render() {
    let tableHeader;
    if (this.state.dashboards.length === 0) {
      tableHeader = "Loading Dashboards...";
    } else {
      tableHeader = `${this.state.dashboards.length} Dashboards`;
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
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-12">
                <div className="panel panel-minimal">
                  <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                    <h2 className="panel-title">{tableHeader}</h2>
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
                                  <td className="monotype">{dashboard.name}</td>
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
      </div>
    );
  },
});

export default DashboardsPage;
