import React, {PropTypes} from 'react';

const {shape, string, number} = PropTypes;
const Task = React.createClass({
  propTypes: {
    task: shape({
      source: string,
      dest: string,
      id: number,
      status: string,
    }).isRequired,
  },

  render() {
    const {id, source, dest, status} = this.props.task;
    return (
      <tr className="task" id={`task-${id}`}>
        <td><div className={`status-dot status-dot__${status.toLowerCase()}`}></div></td>
        <td>Copy Shard</td>{/* TODO: Copy Shard is hardcoded, change when he have more types of tasks */}
        <td>{source}</td>
        <td>{dest}</td>
        {/* TODO: add killing a task into app when it exists in backend
          <td className="text-right"><a href="#" data-toggle="modal" data-task-id={id} data-target="#killModal">Kill</a></td>
         */}
      </tr>
    );
  },
});

export default Task;
