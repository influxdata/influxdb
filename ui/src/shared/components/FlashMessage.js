import React, {PropTypes} from 'react';

const {string, oneOfType, shape, arrayOf} = PropTypes;

const messageClasses = {
  error: 'danger',
  success: 'success',
};

export default React.createClass({
  propTypes: {
    message: shape({
      text: oneOfType([string, arrayOf(string)]),
      type: string.isRequired,
    }),
  },

  getInitialState() {
    return {show: true};
  },

  handleClick() {
    this.setState({show: false});
  },

  render() {
    if (!this.state.show) {
      return null;
    }

    const {text, type} = this.props.message;
    return (
      <div className={`alert alert-${messageClasses[type]}`} role="alert">
        {Array.isArray(text) ? (
          <ul>
            {text.map((msg, i) => <li key={i}>{msg}</li>)}
          </ul>
        ) : text}
        <button className="close" data-dismiss="alert" aria-label="Close" onClick={this.handleClick}>
          <span className="icon remove"></span>
        </button>
      </div>
    );
  },
});
