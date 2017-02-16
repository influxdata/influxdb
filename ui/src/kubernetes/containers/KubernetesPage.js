import React, {PropTypes} from 'react';
import {connect} from 'react-redux'

import {fetchLayouts} from 'shared/apis';
import KubernetesDashboard from 'src/kubernetes/components/KubernetesDashboard';
import {delayEnablePresentationMode} from 'shared/actions/ui';

const {
  shape,
  string,
  bool,
  func,
} = PropTypes

export const KubernetesPage = React.createClass({
  propTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }),
    inPresentationMode: bool.isRequired,
    handleClickPresentationButton: func,
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
    const {layouts} = this.state
    const {source, inPresentationMode, handleClickPresentationButton} = this.props

    return (
      <KubernetesDashboard
        layouts={layouts}
        source={source}
        inPresentationMode={inPresentationMode}
        handleClickPresentationButton={handleClickPresentationButton}
      />
    );
  },
});

const mapStateToProps = (state) => ({
  inPresentationMode: state.appUI.presentationMode,
})

const mapDispatchToProps = (dispatch) => ({
  handleClickPresentationButton: () => {
    dispatch(delayEnablePresentationMode())
  },
})

export default connect(mapStateToProps, mapDispatchToProps)(KubernetesPage);
