import React, {PropTypes} from 'react';
import classNames from 'classnames';
import QueryEditor from './QueryEditor';
import QueryTabItem from './QueryTabItem';
import RenamePanelModal from './RenamePanelModal';

const {shape, func, bool, arrayOf} = PropTypes;
const Explorer = React.createClass({
  propTypes: {
    panel: shape({}).isRequired,
    queries: arrayOf(shape({})).isRequired,
    timeRange: PropTypes.shape({
      upper: PropTypes.string,
      lower: PropTypes.string,
    }).isRequired,
    isExpanded: bool.isRequired,
    onToggleExplorer: func.isRequired,
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
      activeQueryId: null,
    };
  },

  handleSetActiveQuery(query) {
    this.setState({activeQueryId: query.id});
  },

  handleAddQuery() {
    this.props.actions.addQuery();
  },

  handleDeleteQuery(query) {
    this.props.actions.deleteQuery(query.id);
  },

  handleSelectExplorer() {
    this.props.onToggleExplorer(this.props.panel);
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
      <div className={classNames('explorer', {active: isExpanded})}>
        <div className="explorer--header" onClick={this.handleSelectExplorer}>
          <div className="explorer--name">
            <span className="icon caret-right"></span>
            {panel.name || "Graph"}
          </div>
          <div className="explorer--actions">
            <div title="Rename Graph" className="explorer--action" onClick={this.openRenamePanelModal}><span className="icon pencil"></span></div>
            <div title="Delete Graph" className="explorer--action" onClick={this.handleDeletePanel}><span className="icon trash"></span></div>
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
        <div className="query-editor__empty">
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
    if (!this.props.isExpanded) {
      return null;
    }

    return (
      <div className="explorer--tabs">
        {this.props.queries.map((q) => {
          return (
            <QueryTabItem
              isActive={this.getActiveQuery().id === q.id}
              key={q.id}
              query={q}
              onSelect={this.handleSetActiveQuery}
              onDelete={this.handleDeleteQuery}
            />
          );
        })}
        <div className="explorer--tab" onClick={this.handleAddQuery}>
          <span className="icon plus"></span>
        </div>
      </div>
    );
  },
});

export default Explorer;
