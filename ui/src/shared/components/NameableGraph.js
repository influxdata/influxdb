import React, {PropTypes} from 'react'

const NameableGraph = React.createClass({
  propTypes: {
    cell: PropTypes.shape({
      name: PropTypes.string.isRequired,
      id: PropTypes.string.isRequired,
    }).isRequired,
    children: PropTypes.node.isRequired,
    onRename: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      editing: false,
    }
  },

  handleClick() {
    this.setState({
      editing: !this.state.editing, /* eslint-disable no-negated-condition */
    });
  },

  handleChangeName(evt) {
    this.props.onRename({
      ...this.props.cell,
      name: evt.target.value,
    })
  },

  render() {
    let nameOrField
    if (!this.state.editing) {
      nameOrField = this.props.cell.name
    } else {
      nameOrField = <input type="text" value={this.props.cell.name} autoFocus={true} onChange={this.handleChangeName}></input>
    }

    return (
      <div>
        <h2 className="dash-graph--heading" onClick={this.handleClick}>{nameOrField}</h2>
        <div className="dash-graph--container">
          {this.props.children}
        </div>
      </div>
    );
  },
});

export default NameableGraph;
