import React, {PropTypes} from 'react';

const SourceIndicator = React.createClass({
  propTypes: {
    source: PropTypes.shape({}).isRequired,
  },

  render() {
    const {source} = this.props;
    return (
      <div className="source-indicator">
        <span className="icon server"></span>
        {source && source.name}
      </div>
    );
  },
});

export default SourceIndicator;
