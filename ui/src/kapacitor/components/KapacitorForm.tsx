import React, {SFC, ChangeEvent, MouseEvent} from 'react'

import AlertOutputs from 'src/kapacitor/components/AlertOutputs'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import Input from 'src/kapacitor/components/KapacitorFormInput'

import {Kapacitor, Source} from 'src/types'

type FlashMessage = {type: string; text: string}

interface Props {
  kapacitor: Kapacitor
  exists: boolean
  onReset: (e: MouseEvent<HTMLButtonElement>) => void
  onSubmit: (e: ChangeEvent<HTMLFormElement>) => void
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  onChangeUrl: (e: ChangeEvent<HTMLInputElement>) => void
  addFlashMessage: (message: FlashMessage) => void
  source: Source
  hash: string
}

const KapacitorForm: SFC<Props> = ({
  onChangeUrl,
  onReset,
  kapacitor,
  kapacitor: {url, name, username, password},
  onSubmit,
  exists,
  onInputChange,
  addFlashMessage,
  source,
  hash,
}) =>
  <div className="page">
    <div className="page-header">
      <div className="page-header__container">
        <div className="page-header__left">
          <h1 className="page-header__title">{`${exists
            ? 'Configure'
            : 'Add a New'} Kapacitor Connection`}</h1>
        </div>
      </div>
    </div>
    <FancyScrollbar className="page-contents">
      <div className="container-fluid">
        <div className="row">
          <div className="col-md-3">
            <div className="panel">
              <div className="panel-heading">
                <h2 className="panel-title">Connection Details</h2>
              </div>
              <div className="panel-body">
                <form onSubmit={onSubmit}>
                  <div>
                    <Input
                      name="kapaUrl"
                      label="Kapacitor URL"
                      value={url}
                      placeholder={url}
                      onChange={onChangeUrl}
                    />
                    <Input
                      name="name"
                      label="Name"
                      value={name}
                      placeholder={name}
                      onChange={onInputChange}
                      maxLength={33}
                    />
                    <Input
                      name="username"
                      label="Username"
                      value={username || ''}
                      placeholder={username}
                      onChange={onInputChange}
                    />
                    <Input
                      name="password"
                      label="password"
                      placeholder="password"
                      value={password || ''}
                      onChange={onInputChange}
                      inputType="password"
                    />
                  </div>
                  <div className="form-group form-group-submit col-xs-12 text-center">
                    <button
                      className="btn btn-default"
                      type="button"
                      onClick={onReset}
                      data-test="reset-button"
                    >
                      Reset
                    </button>
                    <button className="btn btn-success" type="submit">
                      {exists ? 'Update' : 'Connect'}
                    </button>
                  </div>
                </form>
              </div>
            </div>
          </div>
          <div className="col-md-9">
            {
              <AlertOutputs
                exists={exists}
                kapacitor={kapacitor}
                addFlashMessage={addFlashMessage}
                source={source}
                hash={hash}
              />
            }
          </div>
        </div>
      </div>
    </FancyScrollbar>
  </div>

export default KapacitorForm
