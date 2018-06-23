import * as ErrorData from 'src/types/errors'
import * as ErrorActions from 'src/types/actions/error'

export const errorThrown = (
  error: ErrorData.ErrorDescription,
  altText?: string,
  alertType?: ErrorData.AlertType
): ErrorActions.ErrorThrownAction => ({
  type: 'ERROR_THROWN',
  error,
  altText,
  alertType,
})
