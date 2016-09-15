import React, {PropTypes} from 'react';
import classNames from 'classnames';
import selectStatement from '../utils/influxql/select';
import RenamePanelModal from './RenamePanelModal';

const ENTER = 13;

const RawQueryGroup = React.createClass({
  propTypes: {
    queryConfigs: PropTypes.arrayOf(PropTypes.shape({})),
    panel: PropTypes.shape({
      id: PropTypes.string.isRequired,
    }),
    timeRange: PropTypes.shape({
      upper: PropTypes.string,
      lower: PropTypes.string,
    }).isRequired,
    actions: PropTypes.shape({
      updateRawQuery: PropTypes.func.isRequired,
      renamePanel: PropTypes.func.isRequired,
      deletePanel: PropTypes.func.isRequired,
    }),
    activeQueryID: PropTypes.string,
    setActivePanel: PropTypes.func,
  },

  handleRename(newName) {
    this.props.actions.renamePanel(this.props.panel.id, newName);
  },

  handleDeletePanel(e) {
    e.stopPropagation();
    this.props.actions.deletePanel(this.props.panel.id);
  },

  openRenamePanelModal(e) {
    e.stopPropagation();
    $(`#renamePanelModal-${this.props.panel.id}`).modal('show'); // eslint-disable-line no-undef
  },

  handleSetActivePanel() {
    this.props.setActivePanel(this.props.panel.id);
  },

  render() {
    const {queryConfigs, timeRange, panel} = this.props;

    return (
      <div className="raw-editor__panel">
        <RenamePanelModal panel={panel} onConfirm={this.handleRename} />
        <div className="raw-editor__header" onClick={this.handleSelectExplorer}>
          <div className="raw-editor__header-name">
            {panel.name || "Panel"}
          </div>
          <div className="raw-editor__header-actions">
            <div title="Rename" className="raw-editor__header-rename" onClick={this.openRenamePanelModal}><span className="icon pencil"></span></div>
            <div title="Delete" className="raw-editor__header-delete" onClick={this.handleDeletePanel}><span className="icon trash"></span></div>
          </div>
        </div>
        {queryConfigs.map((q) => {
          return <RawQueryEditor onFocus={this.handleSetActivePanel} shouldFocus={this.props.activeQueryID === q.id} key={q.id} query={q} defaultValue={q.rawText || selectStatement(timeRange, q) || ''} updateRawQuery={this.props.actions.updateRawQuery} />;
        })}
      </div>
    );
  },
});

const RawQueryEditor = React.createClass({
  propTypes: {
    query: PropTypes.shape({
      rawText: PropTypes.string,
      id: PropTypes.string.isRequired,
    }).isRequired,
    updateRawQuery: PropTypes.func.isRequired,
    defaultValue: PropTypes.string.isRequired,
    shouldFocus: PropTypes.bool.isRequired,
    onFocus: PropTypes.func.isRequired,
  },

  componentDidMount() {
    if (this.props.shouldFocus) {
      this.editor.scrollIntoView();
      this.editor.focus();
    }
  },

  handleKeyDown(e) {
    e.stopPropagation();
    if (e.keyCode !== ENTER) {
      return;
    }
    e.preventDefault();
    this.editor.blur();
  },

  updateRawQuery() {
    const text = this.editor.value;
    this.props.updateRawQuery(this.props.query.id, text);
  },

  render() {
    const {defaultValue, query} = this.props;

    return (
      <div className={classNames("raw-query-editor-wrapper", {"rq-mode": query.rawText})}>
        <textarea
          className="raw-query-editor"
          onKeyDown={this.handleKeyDown}
          onBlur={this.updateRawQuery}
          onFocus={this.props.onFocus}
          ref={(editor) => this.editor = editor}
          defaultValue={defaultValue}
          placeholder="Blank query"
        />
        {/* <button title="Delete this query?" className="raw-query-editor-delete">
          <span className="icon remove"></span>
          </button> */}
      </div>
    );
  },
});

export default RawQueryGroup;
