import React, {ChangeEvent, MouseEvent, PureComponent} from 'react'

import AlertOutputs from 'src/kapacitor/components/AlertOutputs'
import Input from 'src/kapacitor/components/KapacitorFormInput'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

import {Kapacitor, Source} from 'src/types'
import KapacitorFormSkipVerify from 'src/kapacitor/components/KapacitorFormSkipVerify'

export interface Notification {
  id?: string
  type: string
  icon: string
  duration: number
  message: string
}

type NotificationFunc = () => Notification

interface Props {
  kapacitor: Kapacitor
  exists: boolean
  onReset: (e: MouseEvent<HTMLButtonElement>) => void
  onSubmit: (e: ChangeEvent<HTMLFormElement>) => void
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  onCheckboxChange: (e: ChangeEvent<HTMLInputElement>) => void
  onChangeUrl: (e: ChangeEvent<HTMLInputElement>) => void
  source: Source
  hash: string
  notify: (message: Notification | NotificationFunc) => void
}

class KapacitorForm extends PureComponent<Props> {
  public render() {
    const {
      onChangeUrl,
      onReset,
      kapacitor,
      kapacitor: {name, username, password},
      onSubmit,
      exists,
      onInputChange,
      onCheckboxChange,
      source,
      hash,
      notify,
    } = this.props

    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">{this.headerText}</h1>
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
                          value={this.url}
                          placeholder={this.url}
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
                      {this.isSecure && (
                        <KapacitorFormSkipVerify
                          kapacitor={kapacitor}
                          onCheckboxChange={onCheckboxChange}
                        />
                      )}
                      <div className="form-group form-group-submit col-xs-12 text-center">
                        <button
                          className="btn btn-default"
                          type="button"
                          onClick={onReset}
                          data-test="reset-button"
                        >
                          Reset
                        </button>
                        <button
                          className="btn btn-success"
                          type="submit"
                          data-test="submit-button"
                        >
                          {this.buttonText}
                        </button>
                      </div>
                    </form>
                  </div>
                </div>
              </div>
              <div className="col-md-9">
                <AlertOutputs
                  hash={hash}
                  exists={exists}
                  source={source}
                  kapacitor={kapacitor}
                  notify={notify}
                />
              </div>
            </div>
          </div>
        </FancyScrollbar>
      </div>
    )
  }

  private get buttonText(): string {
    const {exists} = this.props

    if (exists) {
      return 'Update'
    }

    return 'Connect'
  }

  private get headerText(): string {
    const {exists} = this.props

    let prefix = 'Add a New'
    if (exists) {
      prefix = 'Configure'
    }

    return `${prefix} Kapacitor Connection`
  }

  private get url(): string {
    const {
      kapacitor: {url},
    } = this.props
    if (url) {
      return url
    }

    return ''
  }

  private get isSecure(): boolean {
    return this.url.startsWith('https')
  }
}

export default KapacitorForm
