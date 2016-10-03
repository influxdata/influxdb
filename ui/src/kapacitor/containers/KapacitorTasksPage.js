import React, {PropTypes} from 'react';
import FlashMessages from 'shared/components/FlashMessages';

export const KapacitorTasksPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      type: PropTypes.string.isRequired, // 'influx-enterprise'
      username: PropTypes.string.isRequired,
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
    };
  },

  componentDidMount() {
  },

  render() {
    return (
      <div className="kapacitorTasks">
        tasks
      </div>
    );
  },

});

export default FlashMessages(KapacitorTasksPage);
