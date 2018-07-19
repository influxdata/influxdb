import React, {SFC, ChangeEvent} from 'react'
import {Kapacitor} from 'src/types'
import {insecureSkipVerifyText} from 'src/shared/copy/tooltipText'

interface Props {
  kapacitor: Kapacitor
  onCheckboxChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const KapacitorFormSkipVerify: SFC<Props> = ({
  kapacitor: {insecureSkipVerify},
  onCheckboxChange,
}) => {
  return (
    <div className="form-group col-xs-12">
      <div className="form-control-static">
        <input
          type="checkbox"
          id="insecureSkipVerifyCheckbox"
          name="insecureSkipVerify"
          checked={insecureSkipVerify}
          onChange={onCheckboxChange}
        />
        <label htmlFor="insecureSkipVerifyCheckbox">Unsafe SSL</label>
      </div>
      <label className="form-helper">{insecureSkipVerifyText}</label>
    </div>
  )
}

export default KapacitorFormSkipVerify
