import React, {PropTypes} from 'react';

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
      <div className="enterprise-header">
        <div className="enterprise-header__container">
          <div className="enterprise-header__left">
            {this.renderEditName()}
          </div>
          <div className="enterprise-header__right">
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
        <button className="btn btn-sm btn-default disabled" title={validationError}>
          Save Rule
        </button>
      </div>
    );
  },

  renderEditName() {
    const {rule} = this.props;

    if (!this.state.isEditingName) {
      return (
        <h1 className="enterprise-header__editable" onClick={this.toggleEditName}>
          {rule.name}
        </h1>
      );
    }

    return (
      <input
        className="enterprise-header__editing"
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
