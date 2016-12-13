import React from 'react';

const DashboardsPage = React.createClass({
  getInitialState() {
    return {
      dashboards: [],
    };
  },

  componentDidMount() {
    // getDashboards().then((dashboards) => {
    const dashboards = [];
    this.state({
      dashboards,
    });
    // });
  },

  render() {
    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>
                Bond Villain Dashboards
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
                    <h2 className="panel-title">4 Doomsday Devices</h2>
                  </div>
                  <div className="panel-body">
                    <table className="table v-center">
                      <thead>
                        <tr>
                          <th>Name</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr>
                          <td className="monotype">Goldeneye System Status</td>
                        </tr>
                        <tr>
                          <td className="monotype">Carver Media Group Broadcast System Status</td>
                        </tr>
                        <tr>
                          <td className="monotype">King Oil Pipeline Status</td>
                        </tr>
                        <tr>
                          <td className="monotype">Icarus Space Program Status</td>
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
