import React, {SFC} from 'react'

import AlertOutputs from 'src/kapacitor/components/AlertOutputs'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

import {Kapacitor, Source} from 'src/types'

type FlashMessage = {type: string; text: string}

interface Props {
  kapacitor: Kapacitor
  exists: boolean
  onReset: (e: React.SyntheticEvent<HTMLButtonElement>) => void
  onSubmit: (e: React.SyntheticEvent<HTMLFormElement>) => void
  onInputChange: (e: React.SyntheticEvent<HTMLFormElement>) => void
  onChangeUrl: (e: React.SyntheticEvent<HTMLFormElement>) => void
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
                    <div className="form-group">
                      <label htmlFor="kapaUrl">Kapacitor URL</label>
                      <input
                        className="form-control"
                        id="kapaUrl"
                        name="kapaUrl"
                        placeholder={url}
                        value={url}
                        onChange={onChangeUrl}
                        spellCheck={false}
                        data-test="kapaUrl"
                      />
                    </div>
                    <div className="form-group">
                      <label htmlFor="name">Name</label>
                      <input
                        className="form-control"
                        id="name"
                        name="name"
                        placeholder={name}
                        value={name}
                        onChange={onInputChange}
                        spellCheck={false}
                        maxLength={33}
                      />
                    </div>
                    <div className="form-group">
                      <label htmlFor="username">Username</label>
                      <input
                        className="form-control"
                        id="username"
                        name="username"
                        placeholder="username"
                        value={username || ''}
                        onChange={onInputChange}
                        spellCheck={false}
                      />
                    </div>
                    <div className="form-group">
                      <label htmlFor="password">Password</label>
                      <input
                        className="form-control"
                        id="password"
                        type="password"
                        name="password"
                        placeholder="password"
                        value={password || ''}
                        onChange={onInputChange}
                        spellCheck={false}
                      />
                    </div>
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
