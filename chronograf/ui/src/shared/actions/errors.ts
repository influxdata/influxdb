import * as ErrorsActions from 'src/types/actions/errors'
import * as ErrorsModels from 'src/types/errors'

export const errorThrown = (
  error: ErrorsModels.ErrorDescription,
  altText?: string,
  alertType?: ErrorsModels.AlertType
): ErrorsActions.ErrorThrownAction => ({
  type: 'ERROR_THROWN',
  error,
  altText,
  alertType,
})
