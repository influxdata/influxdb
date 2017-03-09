import React, {PropTypes} from 'react'

const NameableGraph = React.createClass({
  propTypes: {
    name: PropTypes.string.isRequired,
    id: PropTypes.string.isRequired,
    children: PropTypes.node.isRequired,
    layout: PropTypes.arrayOf(PropTypes.shape({})).isRequired,
    onRename: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      editing: false,
      name: this.props.name,
    }
  },

  handleClick() {
    this.setState({
      editing: !this.state.editing, /* eslint-disable no-negated-condition */
    });
  },

  handleChangeName(evt) {
    const {editing, name: oldName} = this.state
    const newName = evt.target.value
    const {layout, id, onRename} = this.props

    if (editing && newName !== oldName) {
      const newLayout = layout.map((cell) => {
        if (cell.i === id) {
          const ret = Object.assign({}, cell)
          ret.name = newName
          return ret
        }
        return cell
      })
      onRename(newLayout)
      this.setState({
        name: newName,
      });
    }
  },

  render() {
    let nameOrField
    if (!this.state.editing) {
      nameOrField = this.state.name
    } else {
      nameOrField = <input type="text" value={this.state.name} autoFocus={true} onChange={this.handleChangeName}></input>
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
