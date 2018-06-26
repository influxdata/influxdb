import React, {PureComponent} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'
import TemplatePreviewList from 'src/tempVars/components/TemplatePreviewList'

import {RemoteDataState, TemplateValue} from 'src/types'

interface Props {
  items: TemplateValue[]
  loadingStatus: RemoteDataState
  onUpdateDefaultTemplateValue: (item: TemplateValue) => void
}

@ErrorHandling
class TemplateMetaQueryPreview extends PureComponent<Props> {
  public render() {
    const {items, loadingStatus, onUpdateDefaultTemplateValue} = this.props

    if (loadingStatus === RemoteDataState.NotStarted) {
      return <div className="temp-builder-results" />
    }

    if (loadingStatus === RemoteDataState.Loading) {
      return (
        <div className="temp-builder-results">
          <p className="loading">Loading meta query preview...</p>
        </div>
      )
    }

    if (loadingStatus === RemoteDataState.Error) {
      return (
        <div className="temp-builder-results">
          <p className="error">Meta Query failed to execute</p>
        </div>
      )
    }

    if (items.length === 0) {
      return (
        <div className="temp-builder-results">
          <p className="warning">
            Meta Query is syntactically correct but returned no results
          </p>
        </div>
      )
    }

    const pluralizer = items.length === 1 ? '' : 's'

    return (
      <div className="temp-builder-results">
        <p>
          Meta Query returned <strong>{items.length}</strong> value{pluralizer}
        </p>
        {items.length > 0 && (
          <TemplatePreviewList
            items={items}
            onUpdateDefaultTemplateValue={onUpdateDefaultTemplateValue}
          />
        )}
      </div>
    )
  }
}

export default TemplateMetaQueryPreview
