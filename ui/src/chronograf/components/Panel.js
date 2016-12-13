import React, {PropTypes} from 'react';
import classNames from 'classnames';
import QueryEditor from './QueryEditor';
import QueryTabItem from './QueryTabItem';
import RenamePanelModal from './RenamePanelModal';
import SimpleDropdown from 'src/shared/components/SimpleDropdown';

<<<<<<< HEAD:ui/src/chronograf/components/Panel.js
const {shape, func, bool, arrayOf} = PropTypes;
const Panel = React.createClass({
=======
const Explorer = React.createClass({
>>>>>>> Move activeQueryID state to DE container:ui/src/chronograf/components/Explorer.js
  propTypes: {
    panel: PropTypes.shape({
      id: PropTypes.string.isRequired,
    }).isRequired,
    queries: PropTypes.arrayOf(PropTypes.shape({})).isRequired,
    timeRange: PropTypes.shape({
      upper: PropTypes.string,
      lower: PropTypes.string,
    }).isRequired,
    isExpanded: PropTypes.bool.isRequired,
    onToggleExplorer: PropTypes.func.isRequired,
    actions: PropTypes.shape({
      chooseNamespace: PropTypes.func.isRequired,
      chooseMeasurement: PropTypes.func.isRequired,
      chooseTag: PropTypes.func.isRequired,
      groupByTag: PropTypes.func.isRequired,
      addQuery: PropTypes.func.isRequired,
      deleteQuery: PropTypes.func.isRequired,
      toggleField: PropTypes.func.isRequired,
      groupByTime: PropTypes.func.isRequired,
      toggleTagAcceptance: PropTypes.func.isRequired,
      applyFuncsToField: PropTypes.func.isRequired,
      deletePanel: PropTypes.func.isRequired,
      renamePanel: PropTypes.func.isRequired,
    }).isRequired,
    setActiveQuery: PropTypes.func.isRequired,
    activeQueryID: PropTypes.string,
  },

  handleSetActiveQuery(query) {
    this.props.setActiveQuery(query.id);
  },

  handleAddQuery() {
    this.props.actions.addQuery();
  },

  handleAddRawQuery() {
    this.props.actions.addQuery({rawText: `SELECT "fields" from "db"."rp"."measurement"`});
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
    const {queries, activeQueryID} = this.props;
    const activeQuery = queries.find((query) => query.id === activeQueryID);
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
            {/* <div title="Export Queries to Dashboard" className="panel--action"><span className="icon export"></span></div> */}
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

  onChoose(item) {
    switch (item.text) {
      case 'Query Builder':
        this.handleAddQuery();
        break;
      case 'Raw Text':
        this.handleAddRawQuery();
        break;
    }
  },

  renderAddQuery() {
    return (
      <SimpleDropdown onChoose={this.onChoose} items={[{text: 'Query Builder'}, {text: 'Raw Text'}]} className="panel--tab-new">
        <span className="icon plus"></span>
      </SimpleDropdown>
    );
  },
});

export default Panel;
