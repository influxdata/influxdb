import * as ErrorData from 'src/types/error'

export type ErrorThrownActionCreator = (
  error: ErrorData.ErrorDescription,
  altText?: string,
  alertType?: ErrorData.AlertType
) => ErrorThrownAction

export interface ErrorThrownAction {
  type: 'ERROR_THROWN'
  error: ErrorData.ErrorDescription
  altText?: string
  alertType?: ErrorData.AlertType
}
