import React, {PropTypes} from 'react';
import {fetchLayouts} from '../apis';
import KubernetesDashboard from 'src/kubernetes/components/KubernetesDashboard';

export const KubernetesPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }),
  },

  getInitialState() {
    return {
      layouts: [],
    };
  },

  componentDidMount() {
    fetchLayouts().then(({data: {layouts}}) => {
      const kubernetesLayouts = layouts.filter((l) => l.app === 'kubernetes');
      this.setState({layouts: kubernetesLayouts});
    });
  },

  render() {
    return (
      <KubernetesDashboard layouts={this.state.layouts} source={this.props.source} />
    );
  },
});

export default KubernetesPage;
