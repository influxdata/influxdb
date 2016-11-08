import React, {PropTypes} from 'react';
import RebalanceModal from '../components/RebalanceModal';
import Task from '../components/Task';
import AJAX from 'utils/ajax';
import {meShow} from 'shared/apis';

const REFRESH_INTERVAL = 2000;
const REBALANCE_PERMISSION = 'Rebalance';

const Tasks = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      clusterID: PropTypes.string.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      isRebalancing: false,
      canRebalance: false,
      tasks: [],
    };
  },

  componentDidMount() {
    this.fetchTasks();
    meShow().then(({data}) => {
      const clusterAccount = data.cluster_links.find((cl) => cl.cluster_id === this.props.params.clusterID);

      if (!clusterAccount) {
        return this.setState({canRebalance: false});
      }

      AJAX({
        url: `/api/int/v1/clusters/${this.props.params.clusterID}/meta/authorized?password=""&resource=""`,
        params: {
          name: clusterAccount.cluster_user,
          permission: REBALANCE_PERMISSION,
        },
      }).then(() => {
        this.setState({canRebalance: true});
      }).catch(() => {
        this.setState({canRebalance: false});
      });
    });
  },

  fetchTasks() {
    AJAX({
      url: '/api/v1/jobs',
    }).then((resp) => {
      const tasks = resp.data;
      this.setState({tasks});

      if (tasks.length) {
        this.setState({isRebalancing: true});
        if (!this.intervalID) {
          this.intervalID = setInterval(() => this.fetchTasks(), REFRESH_INTERVAL);
        }
      } else {
        this.setState({isRebalancing: false});
        clearInterval(this.intervalID);
      }
    });
  },

  handleRebalance() {
    AJAX({
      url: `/clusters/${this.props.params.clusterID}/rebalance`,
      method: 'POST',
    }).then(() => {
      this.fetchTasks();
    }).catch(() => {
      // TODO: render flash message
    });
  },

  renderRebalanceButton() {
    if (!this.state.canRebalance) {
      return null;
    }

    if (this.state.isRebalancing) {
      return (
        <div disabled={true} className="btn btn-sm btn-success rebalance">
          <div>Rebalancing</div>
          <div id="rebalance" className="icon sync"/>
        </div>
      );
    }

    return (
      <button className="btn btn-sm btn-primary rebalance" data-toggle="modal" data-target="#rebalanceModal">
        Rebalance
      </button>
    );
  },

  render() {
    const {tasks} = this.state;
    return (
      <div>
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                Tasks
              </h1>
            </div>
            <div className="enterprise-header__right">
              {this.renderRebalanceButton()}
            </div>
          </div>
        </div>
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">

              <div className="panel panel-minimal">
                <div className="panel-heading u-flex u-jc-space-between u-ai-center">
                  <h2 className="panel-title">Running Tasks</h2>
                  <div className="dot-container">
                    <ul className="dot-legend">
                      {/* TODO: add back in when task histrory is introduced
                        <li>
                          <div className="status-dot status-dot__finished">
                          </div>
                          Finished
                        </li>
                      */}
                      <li>
                        <div className="status-dot status-dot__running">
                        </div>
                        Running
                      </li>
                      <li>
                        <div className="status-dot status-dot__planned">
                        </div>
                        Planned
                      </li>
                      {/* TODO: add back in when task histrory is introduced
                      <li>
                        <div className="status-dot status-dot__failed">
                        </div>
                        Failed
                      </li>
                      */}
                    </ul>
                  </div>
                </div>
                {tasks.length ?
                  <div className="panel-body">
                    <table className="table task-table">
                      <thead>
                        <tr>
                          <th>Status</th>
                          <th>Type</th>
                          <th>Source</th>
                          <th>Destination</th>
                          <th></th>
                        </tr>
                      </thead>
                      <tbody>
                        {tasks.map((task, index) => <Task key={index} task={task} />)}
                      </tbody>
                    </table>
                  </div> :
                  <div className="panel-body">
                    <div className="generic-empty-state tasks-empty-state">
                      <span className="icon cubo-node"></span>
                      <h4>No tasks running</h4>
                    </div>
                  </div>
                }
              </div>
            </div>
          </div>
        </div>
        <RebalanceModal onConfirmRebalance={this.handleRebalance} />
      </div>
    );
  },
});

export default Tasks;
