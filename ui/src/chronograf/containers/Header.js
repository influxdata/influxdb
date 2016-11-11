import React, {PropTypes} from 'react';
import moment from 'moment';
import {withRouter} from 'react-router';
import TimeRangeDropdown from '../../shared/components/TimeRangeDropdown';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';
import Dropdown from 'shared/components/Dropdown';

const Header = React.createClass({
  propTypes: {
    timeRange: PropTypes.shape({
      upper: PropTypes.string,
      lower: PropTypes.string,
    }).isRequired,
    explorers: PropTypes.shape({}).isRequired,
    explorerID: PropTypes.string.isRequired,
    actions: PropTypes.shape({
      setTimeRange: PropTypes.func.isRequired,
      createExploration: PropTypes.func.isRequired,
      chooseExploration: PropTypes.func.isRequired,
      deleteExplorer: PropTypes.func.isRequired,
      editExplorer: PropTypes.func.isRequired,
    }),
    router: React.PropTypes.shape({
      push: React.PropTypes.func.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      explorerIDToDelete: null,
      explorerIDToEdit: null,
    };
  },

  contextTypes: {
    source: PropTypes.shape({
      name: PropTypes.string,
    }),
  },

  handleChooseTimeRange(bounds) {
    this.props.actions.setTimeRange(bounds);
  },

  findSelected({upper, lower}) {
    if (upper && lower) {
      const format = (t) => moment(t.replace(/\'/g, '')).format('YYYY-MM-DD HH:mm');
      return `${format(lower)} - ${format(upper)}`;
    }

    const selected = timeRanges.find((range) => range.queryValue === lower);
    return selected ? selected.inputValue : 'Custom';
  },

  handleCreateExploration() {
    // TODO: passing in this.props.router.push is a big smell, getting something like
    // react-router-redux might be better here

    this.props.actions.createExploration(this.context.source, this.props.router.push);
  },

  handleChooseExplorer({id}) {
    if (id === this.props.explorerID) {
      return;
    }

    this.props.actions.chooseExploration(id, this.context.source, this.props.router.push);
  },

  /**
   * As far as modals, bootstrap handles opening and closing, and we just need to
   * store an id for whichever explorer was chosen.
   */
  openDeleteExplorerModal({id}) {
    this.setState({explorerIDToDelete: id});
  },

  confirmDeleteExplorer() {
    this.props.actions.deleteExplorer(
      this.context.source,
      this.state.explorerIDToDelete,
      this.props.router.push
    );
  },

  openEditExplorerModal({id}) {
    this.setState({explorerIDToEdit: id});
  },

  confirmEditExplorer({name}) {
    this.props.actions.editExplorer(this.state.explorerIDToEdit, {name});
  },

  getName({name, createdAt}) {
    return name || `${moment(createdAt).format('MMM-DD-YY — hh:mm:ss')}`;
  },

  render() {
    const {timeRange, explorers, explorerID} = this.props;

    const selectedExplorer = explorers[explorerID];
    const dropdownItems = Object.keys(explorers).map((id) => {
      const ex = explorers[id];
      return {text: this.getName(ex), id: ex.id};
    });
    const dropdownActions = [
      {text: 'Rename', icon: 'pencil', target: '#editExplorerModal', handler: this.openEditExplorerModal},
      {text: 'Delete', icon: 'trash', target: '#deleteExplorerModal', handler: this.openDeleteExplorerModal},
    ];
    return (
      <div className="enterprise-header data-explorer__header">
        <div className="enterprise-header__left">
          <h1 className="dropdown-title">Exploration:</h1>
          <Dropdown
            className="sessions-dropdown"
            items={dropdownItems}
            actions={dropdownActions}
            onChoose={this.handleChooseExplorer}
            selected={this.getName(selectedExplorer)}
          />
          <div className="btn btn-sm btn-primary sessions-dropdown__btn" onClick={this.handleCreateExploration}>New Exploration</div>
        </div>
        <div className="enterprise-header__right">
          <h1>Source:</h1>
          <div className="source-indicator">
            <span className="icon cpu"></span>
            {this.context.source.name}
          </div>
          <h1>Range:</h1>
          <TimeRangeDropdown onChooseTimeRange={this.handleChooseTimeRange} selected={this.findSelected(timeRange)} />
          {/* Placeholder for export functionality
              <a href="#" className="btn btn-sm btn-info">Export</a> */}
          {/* Placeholder for create graph functionality
              <a href="#" className="btn btn-sm btn-primary"><span className="icon graphline"></span>&nbsp;&nbsp;Create Graph</a> */}
        </div>
        <DeleteExplorerModal onConfirm={this.confirmDeleteExplorer} />
        <EditExplorerModal onConfirm={this.confirmEditExplorer} />
      </div>
    );
  },
});

const DeleteExplorerModal = React.createClass({
  propTypes: {
    onConfirm: PropTypes.func.isRequired,
  },

  render() {
    return (
      <div className="modal fade" id="deleteExplorerModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">Are you sure?</h4>
            </div>
            <div className="modal-footer">
              <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
              <button onClick={this.handleConfirm} className="btn btn-danger" type="button" data-dismiss="modal">Confirm</button>
            </div>
          </div>
        </div>
      </div>
    );
  },

  handleConfirm(e) {
    e.preventDefault();

    this.props.onConfirm();
  },
});

const EditExplorerModal = React.createClass({
  propTypes: {
    onConfirm: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {error: null};
  },

  render() {
    return (
      <div className="modal fade" id="editExplorerModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">Rename Exploration</h4>
            </div>
            <form onSubmit={this.handleConfirm}>
              <div className="modal-body">
                {this.state.error ? <div className="alert alert-danger" role="alert">{this.state.error}</div> : null}
                <div className="form-grid padding-top">
                  <div className="form-group col-md-8 col-md-offset-2">
                    <input ref="name" name="renameExplorer" type="text" className="form-control input-lg" id="renameExplorer" required={true} />
                  </div>
                </div>
              </div>
              <div className="modal-footer">
                <button className="btn btn-default" onClick={this.handleCancel}>Cancel</button>
                <input type="submit" value="Rename" className="btn btn-success" />
              </div>
            </form>
          </div>
        </div>
      </div>
    );
  },

  // We can't use `data-dismiss="modal"` because pressing the enter key will
  // close the modal instead of submitting the form.
  handleCancel() {
    $('#editExplorerModal').modal('hide'); // eslint-disable-line no-undef
  },

  handleConfirm(e) {
    e.preventDefault();
    const name = this.refs.name.value;

    if (name === '') {
      this.setState({error: "Name can't be blank"});
      return;
    }

    $('#editExplorerModal').modal('hide'); // eslint-disable-line no-undef
    this.refs.name.value = '';
    this.setState({error: null});
    this.props.onConfirm({name});
  },
});

export default withRouter(Header);
