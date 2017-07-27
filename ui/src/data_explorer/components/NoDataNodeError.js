import React from 'react'
import ClusterError from 'shared/components/ClusterError'
import PanelHeading from 'shared/components/PanelHeading'
import PanelBody from 'shared/components/PanelBody'
import errorCopy from 'hson!shared/copy/errors.hson'

const NoDataNodeError = React.createClass({
  render() {
    return (
      <ClusterError>
        <PanelHeading>
          {errorCopy.noData.head}
        </PanelHeading>
        <PanelBody>
          {errorCopy.noData.body}
        </PanelBody>
      </ClusterError>
    )
  },
})

export default NoDataNodeError
