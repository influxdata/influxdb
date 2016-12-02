import React, {PropTypes} from 'react';
import ReactTooltip from 'react-tooltip';

export const RuleHeader = React.createClass({
  propTypes: {
    onSave: PropTypes.func.isRequired,
    rule: PropTypes.shape({}).isRequired,
    actions: PropTypes.shape({
      updateRuleName: PropTypes.func.isRequired,
    }).isRequired,
    validationError: PropTypes.string.isRequired,
  },

  getInitialState() {
    return {
      isEditingName: false,
    };
  },

  handleEditName(e, rule) {
    if (e.key === 'Enter') {
      const {updateRuleName} = this.props.actions;
      const name = this.ruleName.value;
      updateRuleName(rule.id, name);
      this.toggleEditName();
    }

    if (e.key === 'Escape') {
      this.toggleEditName();
    }
  },

  handleEditNameBlur(rule) {
    const {updateRuleName} = this.props.actions;
    const name = this.ruleName.value;
    updateRuleName(rule.id, name);
    this.toggleEditName();
  },

  toggleEditName() {
    this.setState({isEditingName: !this.state.isEditingName});
  },


  render() {
    return (
      <div className="page-header">
        <div className="page-header__container">
          <div className="page-header__left">
            {this.renderEditName()}
          </div>
          <div className="page-header__right">
            {this.renderSave()}
          </div>
        </div>
      </div>
    );
  },

  renderSave() {
    const {validationError, onSave} = this.props;
    if (!validationError) {
      return <button className="btn btn-success btn-sm" onClick={onSave}>Save Rule</button>;
    }

    return (
      <div>
        <button className="btn btn-sm btn-default disabled" data-for="save-kapacitor-tooltip" data-tip={validationError}>
          Save Rule
        </button>
        <ReactTooltip id="save-kapacitor-tooltip" effect="solid" html={true} offset={{top: 2}} place="bottom" class="influx-tooltip kapacitor-tooltip place-bottom" />
      </div>
    );
  },

  renderEditName() {
    const {rule} = this.props;

    if (!this.state.isEditingName) {
      return (
        <h1 className="chronograf-header__editable" onClick={this.toggleEditName} data-for="rename-kapacitor-tooltip" data-tip="Click to Rename">
          {rule.name}
          <span className="icon pencil"></span>
          <ReactTooltip id="rename-kapacitor-tooltip" delayShow="200" effect="solid" html={true} offset={{top: 2}} place="bottom" class="influx-tooltip kapacitor-tooltip place-bottom" />
        </h1>
      );
    }

    return (
      <input
        className="chronograf-header__editing"
        autoFocus={true}
        defaultValue={rule.name}
        ref={r => this.ruleName = r}
        onKeyDown={(e) => this.handleEditName(e, rule)}
        onBlur={() => this.handleEditNameBlur(rule)}
        placeholder="Name your rule"
      />
    );
  },

});

export default RuleHeader;
