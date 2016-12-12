import React, {PropTypes} from 'react';
import classNames from 'classnames';
import QueryEditor from './QueryEditor';
import QueryTabItem from './QueryTabItem';
import RenamePanelModal from './RenamePanelModal';

const {shape, func, bool, arrayOf} = PropTypes;
const Panel = React.createClass({
  propTypes: {
    panel: shape({}).isRequired,
    queries: arrayOf(shape({})).isRequired,
    timeRange: PropTypes.shape({
      upper: PropTypes.string,
      lower: PropTypes.string,
    }).isRequired,
    isExpanded: bool.isRequired,
    onTogglePanel: func.isRequired,
    actions: shape({
      chooseNamespace: func.isRequired,
      chooseMeasurement: func.isRequired,
      chooseTag: func.isRequired,
      groupByTag: func.isRequired,
      addQuery: func.isRequired,
      deleteQuery: func.isRequired,
      toggleField: func.isRequired,
      groupByTime: func.isRequired,
      toggleTagAcceptance: func.isRequired,
      applyFuncsToField: func.isRequired,
      deletePanel: func.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      showAddQueryOptions: false,
      activeQueryId: null,
    };
  },

  handleSetActiveQuery(query) {
    this.setState({activeQueryId: query.id});
  },

  toggleAddQueryOptions() {
    this.setState({showAddQueryOptions: !this.state.showAddQueryOptions});
  },

  handleAddQuery() {
    this.props.actions.addQuery();
    this.setState({showAddQueryOptions: false});
  },

  handleAddRawQuery() {
    this.props.actions.addQuery({rawText: `SELECT "fields" from "db"."rp"."measurement"`});
    this.setState({showAddQueryOptions: false});
  },

  handleDeleteQuery(query) {
    this.props.actions.deleteQuery(query.id);
  },

  handleSelectPanel() {
    this.props.onTogglePanel(this.props.panel);
  },

  handleDeletePanel(e) {
    e.stopPropagation();
    this.props.actions.deletePanel(this.props.panel.id);
  },

  getActiveQuery() {
    const {queries} = this.props;
    const activeQuery = queries.find((query) => query.id === this.state.activeQueryId);
    const defaultQuery = queries[0];

    return activeQuery || defaultQuery;
  },

  openRenamePanelModal(e) {
    e.stopPropagation();
    $(`#renamePanelModal-${this.props.panel.id}`).modal('show'); // eslint-disable-line no-undef
  },

  handleRename(newName) {
    this.props.actions.renamePanel(this.props.panel.id, newName);
  },


  render() {
    const {panel, isExpanded} = this.props;

    return (
      <div className={classNames('panel', {active: isExpanded})}>
        <div className="panel--header" onClick={this.handleSelectPanel}>
          <div className="panel--name">
            <span className="icon caret-right"></span>
            {panel.name || "Graph"}
          </div>
          <div className="panel--actions">
            <div title="Export Queries to Dashboard" className="panel--action"><span className="icon export"></span></div>
            <div title="Rename Graph" className="panel--action" onClick={this.openRenamePanelModal}><span className="icon pencil"></span></div>
            <div title="Delete Graph" className="panel--action" onClick={this.handleDeletePanel}><span className="icon trash"></span></div>
          </div>
        </div>
        {this.renderQueryTabList()}
        {this.renderQueryEditor()}
        <RenamePanelModal panel={panel} onConfirm={this.handleRename} />
      </div>
    );
  },

  renderQueryEditor() {
    if (!this.props.isExpanded) {
      return null;
    }

    const {timeRange, actions} = this.props;
    const query = this.getActiveQuery();

    if (!query) {
      return (
        <div className="qeditor--empty">
          <h5>This Graph has no Queries</h5>
          <br/>
          <div className="btn btn-primary" role="button" onClick={this.handleAddQuery}>Add a Query</div>
        </div>
      );
    }

    return (
      <QueryEditor
        timeRange={timeRange}
        query={this.getActiveQuery()}
        actions={actions}
        onAddQuery={this.handleAddQuery}
      />
    );
  },

  renderQueryTabList() {
    const {isExpanded, queries} = this.props;
    if (!isExpanded) {
      return null;
    }
    return (
      <div className="panel--tabs">
        {queries.map((q) => {
          const queryTabText = (q.measurement && q.fields.length !== 0) ? `${q.measurement}.${q.fields[0].field}` : 'Query';
          return (
            <QueryTabItem
              isActive={this.getActiveQuery().id === q.id}
              key={q.id}
              query={q}
              onSelect={this.handleSetActiveQuery}
              onDelete={this.handleDeleteQuery}
              queryTabText={queryTabText}
            />
          );
        })}

        {this.renderAddQuery()}
      </div>
    );
  },

  renderAddQuery() {
    const {showAddQueryOptions} = this.state;
    if (showAddQueryOptions) {
      return (
        <div className="btn-group btn-group-sm">
          <button type="button" className="btn btn-default" onClick={this.handleAddQuery}>Builder</button>
          <button type="button" className="btn btn-default" onClick={this.handleAddRawQuery}>Raw</button>
          <button type="button" className="btn btn-default" onClick={this.toggleAddQueryOptions}>X</button>
        </div>
      );
    }

    return (
      <div className="panel--tab" onClick={this.toggleAddQueryOptions}>
        <span className="icon plus"></span>
      </div>
    );
  },
});

export default Panel;
