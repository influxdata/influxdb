import React, {PropTypes} from 'react';
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {fetchLayouts} from 'shared/apis';
import KubernetesDashboard from 'src/kubernetes/components/KubernetesDashboard';

import {setAutoRefresh} from 'shared/actions/app'
import {presentationButtonDispatcher} from 'shared/dispatchers'

const {
  bool,
  func,
  number,
  shape,
  string,
} = PropTypes

export const KubernetesPage = React.createClass({
  propTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }),
    autoRefresh: number.isRequired,
    handleChooseAutoRefresh: func.isRequired,
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
    const {source, autoRefresh, handleChooseAutoRefresh, inPresentationMode, handleClickPresentationButton} = this.props

    return (
      <KubernetesDashboard
        layouts={layouts}
        source={source}
        autoRefresh={autoRefresh}
        handleChooseAutoRefresh={handleChooseAutoRefresh}
        inPresentationMode={inPresentationMode}
        handleClickPresentationButton={handleClickPresentationButton}
      />
    );
  },
});

const mapStateToProps = ({app: {ephemeral: {inPresentationMode}, persisted: {autoRefresh}}}) => ({
  inPresentationMode,
  autoRefresh,
})

const mapDispatchToProps = (dispatch) => ({
  handleChooseAutoRefresh: bindActionCreators(setAutoRefresh, dispatch),
  handleClickPresentationButton: presentationButtonDispatcher(dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(KubernetesPage);
